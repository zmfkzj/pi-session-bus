/**
 * Unix-socket endpoint (server) and client helpers of the session bus.
 *
 * SERVER  createEndpoint({busDir, sessionId, getPeerInfo, onNote, limits?}) -> Endpoint
 *   start()  creates/verifies the bus dir (0700), picks the id (derived from sessionId+pid),
 *            listens on <busDir>/<id>.sock (or the private fallback dir for long paths),
 *            chmods the socket 0600 and atomically writes <busDir>/<id>.json (0600).
 *            EADDRINUSE: probe the socket; refused/missing => stale: unlink and retry;
 *            a live (or unresponsive) server => pick another id via a salt.
 *   Each connection carries exactly one request. A connection that exceeds the frame cap
 *   or stays idle longer than idleTimeoutMs without a complete frame is destroyed.
 *   Invalid requests, a wrong `to` and self-addressed notes are answered with a rejection.
 *   Notes are deduplicated by message id (bounded cache): a repeat is answered
 *   `duplicate` with the original wake status and does NOT call onNote again.
 *   stop()   idempotent. Removes the exit listener, answers in-flight requests with
 *            rejected "shutting down", destroys all connections, closes the server and
 *            unlinks socket + registry entry. Resolves once the server is closed.
 *
 * CLIENT  helloProbe(socket, timeout) -> PeerInfo
 *         sendNote(socket, note, timeout) -> NoteDelivered
 *         listPeers(busDir, selfId) -> PeerRecord[]   (probes all entries with hello)
 *   Failures are BusClientError with code unreachable | rejected | timeout | too_large.
 *   `errno` carries the socket error code (e.g. ENOENT/ECONNREFUSED). Only those two
 *   (see isStaleError) make listPeers prune an entry; a timeout never does.
 */

import { chmodSync, unlinkSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import {
  encodeFrame,
  FrameDecoder,
  HELLO_TIMEOUT_MS,
  IDLE_TIMEOUT_MS,
  MAX_CONTENT_BYTES,
  MAX_FRAME_BYTES,
  NOTE_TIMEOUT_MS,
  PROTOCOL_VERSION,
  parseHelloResponse,
  parseNoteResponse,
  parseRequest,
  rejected,
  createHelloRequest,
  type HelloResponse,
  type NoteDelivered,
  type NoteRequest,
  type NoteResponse,
  type PeerInfo,
} from "./protocol.ts";
import {
  deriveId,
  ensurePrivateDir,
  listEntries,
  prepareSocketPath,
  readEntry,
  removeEntry,
  writeEntry,
  type RegistryEntry,
  type SocketPathOptions,
} from "./registry.ts";

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

export type BusClientErrorCode = "unreachable" | "rejected" | "timeout" | "too_large";

export class BusClientError extends Error {
  readonly code: BusClientErrorCode;
  /** Peer-supplied rejection reason (code "rejected"). */
  readonly reason: string | undefined;
  /** Socket error code such as ENOENT / ECONNREFUSED (code "unreachable"). */
  readonly errno: string | undefined;

  constructor(code: BusClientErrorCode, message: string, extra: { reason?: string; errno?: string } = {}) {
    super(message);
    this.name = "BusClientError";
    this.code = code;
    this.reason = extra.reason;
    this.errno = extra.errno;
  }
}

/** True when the failure proves that nobody listens on the socket any more (safe to prune). */
export function isStaleError(err: unknown): boolean {
  return (
    err instanceof BusClientError &&
    err.code === "unreachable" &&
    (err.errno === "ENOENT" || err.errno === "ECONNREFUSED")
  );
}

/** Send one frame and resolve with the first response frame (as text). */
function exchange(socketPath: string, frame: Buffer, timeoutMs: number): Promise<string> {
  return new Promise<string>((resolve, reject) => {
    let settled = false;
    const decoder = new FrameDecoder(MAX_FRAME_BYTES);
    const sock = createConnection(socketPath);
    const finish = (err: BusClientError | undefined, value?: string): void => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      sock.destroy();
      if (err) reject(err);
      else resolve(value as string);
    };
    const timer = setTimeout(
      () => finish(new BusClientError("timeout", `no response from ${socketPath} within ${timeoutMs} ms`)),
      timeoutMs,
    );
    sock.on("error", (err: NodeJS.ErrnoException) => {
      const extra = err.code === undefined ? {} : { errno: err.code };
      finish(new BusClientError("unreachable", `cannot reach ${socketPath}: ${err.message}`, extra));
    });
    sock.on("data", (chunk) => {
      const result = decoder.push(chunk);
      if (result.frames.length > 0) finish(undefined, result.frames[0]);
      else if (result.error) finish(new BusClientError("rejected", `invalid response from peer (${result.error})`));
    });
    sock.on("close", () => finish(new BusClientError("unreachable", "connection closed without a response")));
    sock.write(frame);
  });
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    throw new BusClientError("rejected", "invalid response from peer: not JSON", { reason: "invalid JSON" });
  }
}

export async function helloProbe(socketPath: string, timeoutMs: number = HELLO_TIMEOUT_MS): Promise<PeerInfo> {
  const text = await exchange(socketPath, encodeFrame(createHelloRequest()), timeoutMs);
  const parsed = parseHelloResponse(parseJson(text));
  if (!parsed.ok) throw new BusClientError("rejected", `hello failed: ${parsed.reason}`, { reason: parsed.reason });
  return parsed.value.peer;
}

export interface SendNoteOptions {
  maxContentBytes?: number;
  maxFrameBytes?: number;
}

/**
 * Deliver one note. Resolves with the success response (`delivered` or `duplicate`);
 * a peer rejection throws BusClientError("rejected") with `reason`.
 * Content over the content cap, or a frame over the frame cap, throws "too_large" before connecting.
 */
export async function sendNote(
  socketPath: string,
  note: NoteRequest,
  timeoutMs: number = NOTE_TIMEOUT_MS,
  options: SendNoteOptions = {},
): Promise<NoteDelivered> {
  const maxContent = options.maxContentBytes ?? MAX_CONTENT_BYTES;
  const contentBytes = Buffer.byteLength(note.content, "utf8");
  if (contentBytes > maxContent) {
    throw new BusClientError("too_large", `message content is ${contentBytes} bytes; the limit is ${maxContent}`);
  }
  let frame: Buffer;
  try {
    frame = encodeFrame(note, options.maxFrameBytes ?? MAX_FRAME_BYTES);
  } catch (err) {
    throw new BusClientError("too_large", `message does not fit in one frame: ${(err as Error).message}`);
  }
  const text = await exchange(socketPath, frame, timeoutMs);
  const parsed = parseNoteResponse(parseJson(text));
  if (!parsed.ok) {
    throw new BusClientError("rejected", `invalid response from peer: ${parsed.reason}`, { reason: parsed.reason });
  }
  const res = parsed.value;
  if (!res.ok) throw new BusClientError("rejected", `peer rejected the note: ${res.reason}`, { reason: res.reason });
  return res;
}

/** A live peer as seen through its hello answer, plus where to reach it. */
export interface PeerRecord extends PeerInfo {
  socket: string;
  startedAt: string;
}

export interface ListPeersOptions {
  helloTimeoutMs?: number;
  /** Delete registry entries of dead peers (ENOENT/ECONNREFUSED only). Default true. */
  prune?: boolean;
}

/**
 * Probe every registry entry except `selfId` with hello, in parallel. Returns the peers that
 * answered (oldest first). Dead entries (ENOENT/ECONNREFUSED) are pruned; slow, rejecting
 * or mismatching peers are skipped but never pruned.
 */
export async function listPeers(busDir: string, selfId: string | undefined, options: ListPeersOptions = {}): Promise<PeerRecord[]> {
  const timeout = options.helloTimeoutMs ?? HELLO_TIMEOUT_MS;
  const prune = options.prune ?? true;
  const entries = listEntries(busDir).filter((e) => e.id !== selfId);
  const results = await Promise.all(
    entries.map(async (entry): Promise<PeerRecord | undefined> => {
      try {
        const peer = await helloProbe(entry.socket, timeout);
        if (peer.id !== entry.id) return undefined;
        return { ...peer, socket: entry.socket, startedAt: entry.startedAt };
      } catch (err) {
        if (prune && isStaleError(err)) {
          // Re-read first: a new process may have taken over this id since we listed it.
          const current = readEntry(busDir, entry.id);
          if (current && current.pid === entry.pid && current.startedAt === entry.startedAt) removeEntry(busDir, entry);
        }
        return undefined;
      }
    }),
  );
  return results.filter((r): r is PeerRecord => r !== undefined);
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

export interface EndpointLimits {
  maxFrameBytes?: number;
  maxContentBytes?: number;
  idleTimeoutMs?: number;
  /** Size of the message-id dedup cache (default 1000). */
  dedupSize?: number;
  /** Simultaneous connections accepted (default 128). */
  maxConnections?: number;
}

/** Live data of this session, read on every hello. */
export interface LivePeerInfo {
  name?: string;
  cwd: string;
  busy: boolean;
  autoWake: boolean;
}

export interface EndpointOptions {
  busDir: string;
  sessionId: string;
  /** Override the derived first-choice id (tests). A collision with a live endpoint salts the derivation. */
  id?: string;
  pid?: number;
  getPeerInfo: () => LivePeerInfo;
  /** Handle a validated, deduplicated note addressed to this endpoint. */
  onNote: (note: NoteRequest) => NoteResponse | Promise<NoteResponse>;
  limits?: EndpointLimits;
  /** Socket path fallback inputs (env, tmpdir, uid, maxBytes). */
  socketPath?: SocketPathOptions;
  /** Maximum number of ids tried when sockets are in use (default 16). */
  maxStartAttempts?: number;
  /** Timeout of the EADDRINUSE liveness probe (default HELLO_TIMEOUT_MS). */
  probeTimeoutMs?: number;
  now?: () => number;
}

export interface Endpoint {
  /** Current id (may change during start() when a live endpoint holds the first choice). */
  readonly id: string;
  readonly socketPath: string | undefined;
  readonly running: boolean;
  /** Registry entry of the running endpoint. */
  readonly entry: RegistryEntry | undefined;
  start(): Promise<RegistryEntry>;
  stop(): Promise<void>;
}

type ConnState = "reading" | "processing" | "done";

interface Conn {
  socket: Socket;
  state: ConnState;
  timer: NodeJS.Timeout | undefined;
}

function unlinkQuiet(path: string): void {
  try {
    unlinkSync(path);
  } catch {
    /* already gone */
  }
}

type ListenResult = { ok: true; server: Server } | { ok: false; inUse: true };

export function createEndpoint(options: EndpointOptions): Endpoint {
  const busDir = options.busDir;
  const pid = options.pid ?? process.pid;
  const now = options.now ?? Date.now;
  const maxFrameBytes = options.limits?.maxFrameBytes ?? MAX_FRAME_BYTES;
  const maxContentBytes = options.limits?.maxContentBytes ?? MAX_CONTENT_BYTES;
  const idleTimeoutMs = options.limits?.idleTimeoutMs ?? IDLE_TIMEOUT_MS;
  const dedupSize = Math.max(1, options.limits?.dedupSize ?? 1000);
  const maxConnections = options.limits?.maxConnections ?? 128;
  const maxStartAttempts = options.maxStartAttempts ?? 16;
  const probeTimeoutMs = options.probeTimeoutMs ?? HELLO_TIMEOUT_MS;

  let id = options.id ?? deriveId(options.sessionId, pid);
  let socketPath: string | undefined;
  let entry: RegistryEntry | undefined;
  let server: Server | undefined;
  let state: "new" | "starting" | "running" | "stopped" = "new";
  let stopRequested = false;
  let startPromise: Promise<RegistryEntry> | undefined;
  let stopPromise: Promise<void> | undefined;
  let exitListener: (() => void) | undefined;
  const conns = new Set<Conn>();
  const dedup = new Map<string, Promise<NoteResponse>>();

  // -- connections ---------------------------------------------------------

  function clearConnTimer(conn: Conn): void {
    if (conn.timer) clearTimeout(conn.timer);
    conn.timer = undefined;
  }

  /** Send the single response and close our side; a stalled peer is dropped after idleTimeoutMs. */
  function respond(conn: Conn, response: HelloResponse | NoteResponse, destroyWhenFlushed = false): void {
    if (conn.state === "done") return;
    conn.state = "done";
    clearConnTimer(conn);
    const sock = conn.socket;
    if (sock.destroyed) return;
    let frame: Buffer;
    try {
      frame = encodeFrame(response, Math.max(maxFrameBytes, MAX_FRAME_BYTES));
    } catch {
      sock.destroy();
      return;
    }
    sock.end(frame, destroyWhenFlushed ? () => sock.destroy() : undefined);
    conn.timer = setTimeout(() => sock.destroy(), destroyWhenFlushed ? 1000 : idleTimeoutMs);
    conn.timer.unref();
  }

  async function handleNote(note: NoteRequest): Promise<NoteResponse> {
    if (note.to !== id) return rejected(`wrong recipient: this endpoint is ${id}`);
    if (note.from.id === id) return rejected("self-addressed note");
    const cached = dedup.get(note.id);
    if (cached) {
      let first: NoteResponse;
      try {
        first = await cached;
      } catch {
        return rejected("internal error");
      }
      return first.ok ? { ...first, status: "duplicate" } : first;
    }
    const pending = Promise.resolve().then(() => options.onNote(note));
    dedup.set(note.id, pending);
    while (dedup.size > dedupSize) {
      const oldest = dedup.keys().next().value;
      if (oldest === undefined) break;
      dedup.delete(oldest);
    }
    try {
      const result = await pending;
      if (!result.ok) dedup.delete(note.id);
      return result;
    } catch {
      dedup.delete(note.id);
      return rejected("internal error");
    }
  }

  async function handleFrame(conn: Conn, text: string): Promise<void> {
    try {
      let json: unknown;
      try {
        json = JSON.parse(text);
      } catch {
        respond(conn, rejected("invalid JSON"));
        return;
      }
      const parsed = parseRequest(json, { maxContentBytes });
      if (!parsed.ok) {
        respond(conn, rejected(parsed.reason));
        return;
      }
      if (stopRequested) {
        respond(conn, rejected("shutting down"));
        return;
      }
      const req = parsed.value;
      if (req.type === "hello") {
        const live = options.getPeerInfo();
        const peer: PeerInfo = {
          id,
          sessionId: options.sessionId,
          cwd: live.cwd.slice(0, 4096), // keep within the receiver's validation limits
          pid,
          busy: live.busy,
          autoWake: live.autoWake,
          receiving: true,
        };
        if (live.name !== undefined) peer.name = live.name.slice(0, 256);
        respond(conn, { v: PROTOCOL_VERSION, ok: true, peer });
        return;
      }
      respond(conn, await handleNote(req));
    } catch {
      respond(conn, rejected("internal error"));
    }
  }

  function handleConnection(sock: Socket): void {
    const conn: Conn = { socket: sock, state: "reading", timer: undefined };
    conns.add(conn);
    const decoder = new FrameDecoder(maxFrameBytes);
    conn.timer = setTimeout(() => sock.destroy(), idleTimeoutMs);
    conn.timer.unref();
    sock.on("data", (chunk) => {
      if (conn.state !== "reading") return; // one request per connection; ignore the rest
      const result = decoder.push(chunk);
      if (result.error) {
        sock.destroy();
        return;
      }
      const first = result.frames[0];
      if (first !== undefined) {
        conn.state = "processing";
        clearConnTimer(conn);
        void handleFrame(conn, first);
      }
    });
    sock.on("end", () => {
      if (conn.state === "reading") sock.destroy();
    });
    sock.on("error", () => sock.destroy());
    sock.on("close", () => {
      clearConnTimer(conn);
      conns.delete(conn);
    });
  }

  // -- start ---------------------------------------------------------------

  function listenOn(path: string): Promise<ListenResult> {
    return new Promise((resolve, reject) => {
      const srv = createServer({ allowHalfOpen: true }, handleConnection);
      srv.maxConnections = maxConnections;
      srv.once("error", (err: NodeJS.ErrnoException) => {
        if (err.code === "EADDRINUSE") resolve({ ok: false, inUse: true });
        else reject(err);
      });
      srv.listen(path, () => {
        srv.removeAllListeners("error");
        srv.on("error", () => {
          /* a failing server must not crash the host process */
        });
        resolve({ ok: true, server: srv });
      });
    });
  }

  /** True when the socket belongs to nobody: connecting is refused or the path vanished. */
  async function isStaleSocket(path: string): Promise<boolean> {
    try {
      await helloProbe(path, probeTimeoutMs);
      return false;
    } catch (err) {
      return isStaleError(err);
    }
  }

  function removeOwnFiles(): void {
    if (!entry || !socketPath) return;
    const current = readEntry(busDir, entry.id);
    // Do not delete files a newer endpoint with the same id wrote after us.
    if (current && (current.pid !== entry.pid || current.startedAt !== entry.startedAt)) return;
    removeEntry(busDir, { id: entry.id, socket: socketPath });
  }

  async function doStart(): Promise<RegistryEntry> {
    ensurePrivateDir(busDir);
    let salt = 0;
    let listening: Server | undefined;
    for (let attempt = 0; attempt < maxStartAttempts && !listening; attempt++) {
      id = salt === 0 ? (options.id ?? deriveId(options.sessionId, pid)) : deriveId(options.sessionId, pid, salt);
      const path = prepareSocketPath(busDir, id, options.socketPath);
      socketPath = path;
      let result = await listenOn(path);
      if (!result.ok && (await isStaleSocket(path))) {
        unlinkQuiet(path);
        result = await listenOn(path);
      }
      if (result.ok) listening = result.server;
      else salt++;
    }
    if (!listening || !socketPath) {
      throw new Error(`could not bind a session-bus socket in ${busDir} after ${maxStartAttempts} attempts`);
    }
    server = listening;
    server.unref();
    try {
      chmodSync(socketPath, 0o600);
      const written: RegistryEntry = {
        v: 1,
        id,
        sessionId: options.sessionId,
        pid,
        socket: socketPath,
        startedAt: new Date(now()).toISOString(),
      };
      writeEntry(busDir, written);
      entry = written;
    } catch (err) {
      await doStop();
      throw err;
    }
    exitListener = () => removeOwnFiles();
    process.on("exit", exitListener);
    if (stopRequested) {
      await doStop();
      throw new Error("session-bus endpoint was stopped while starting");
    }
    state = "running";
    return entry;
  }

  // -- stop ----------------------------------------------------------------

  function doStop(): Promise<void> {
    if (stopPromise) return stopPromise;
    state = "stopped";
    stopRequested = true;
    if (exitListener) process.removeListener("exit", exitListener);
    exitListener = undefined;
    for (const conn of [...conns]) {
      if (conn.state === "processing") respond(conn, rejected("shutting down"), true);
      else conn.socket.destroy();
    }
    removeOwnFiles(); // registry entry first, so nobody lists a peer without a socket
    const srv = server;
    stopPromise = new Promise<void>((resolve) => {
      if (!srv || !srv.listening) {
        resolve();
        return;
      }
      srv.close(() => resolve());
    }).then(() => {
      if (socketPath) unlinkQuiet(socketPath);
    });
    return stopPromise;
  }

  return {
    get id() {
      return id;
    },
    get socketPath() {
      return socketPath;
    },
    get running() {
      return state === "running";
    },
    get entry() {
      return state === "running" ? entry : undefined;
    },
    start(): Promise<RegistryEntry> {
      if (state !== "new") return Promise.reject(new Error(`session-bus endpoint already ${state}`));
      state = "starting";
      startPromise = doStart().catch((err) => {
        if (state === "starting") state = "stopped";
        throw err;
      });
      return startPromise;
    },
    async stop(): Promise<void> {
      if (state === "starting" && startPromise) {
        stopRequested = true;
        await startPromise.catch(() => undefined);
      }
      await doStop();
    },
  };
}
