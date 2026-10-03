/** Repository work queue. Atomic JSON snapshots are authoritative; sockets only nudge. */
import { createHash, randomUUID } from "node:crypto";
import {
  closeSync, constants, fstatSync, lstatSync, openSync, readFileSync,
  renameSync, unlinkSync, writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { helloProbe, isGone, isStaleError, sendQueueNudge } from "./endpoint.ts";
import { ensurePrivateDir, listSockets, type SocketPathOptions } from "./registry.ts";

export interface QueueEntry {
  id: string;
  endpointId: string;
  pid: number;
  sessionId: string;
  name?: string;
  title: string;
  state: "active" | "waiting";
  enqueuedAt: string;
  grantedAt?: string;
  holdExpiresAt: number | null;
}
export interface QueueFile { v: 1; repo: string; entries: QueueEntry[] }
export interface QueueMutation { file: QueueFile; promoted?: QueueEntry }
export interface QueueAcquisition extends QueueMutation {
  entry: QueueEntry;
  /** One-based index, including the holder. */
  position: number;
  holder?: QueueEntry;
}
export interface QueueOwner { endpointId: string; pid: number; sessionId: string; name?: string; title: string }
export interface QueueStoreOptions {
  busDir: string;
  repo: string;
  idleMs?: number;
  now?: () => number;
  /** Lock acquisition deadline uses wall time, never the injectable queue clock. */
  lockTimeoutMs?: number;
}
export interface QueueStore {
  readonly key: string;
  readonly repo: string;
  readonly path: string;
  read(): QueueFile;
  mutate(fn: (file: QueueFile, now: number) => void): Promise<QueueMutation>;
  enqueue(owner: QueueOwner): Promise<QueueAcquisition>;
  removeEntry(endpointId: string, pid: number): Promise<QueueMutation>;
  removeIfSameId(id: string): Promise<QueueMutation>;
  setHoldExpiry(id: string, expiry: number | null): Promise<QueueMutation>;
}

export function queueKey(repo: string): string {
  return createHash("sha256").update(repo).digest("hex").slice(0, 16);
}
/** First prompt line only; control characters never become terminal instructions. */
export function queueTitle(prompt: string): string {
  return (prompt.split(/[\r\n\u2028\u2029]/, 1)[0] ?? "")
    .replace(/[\u0000-\u001f\u007f-\u009f]/g, " ").trim().slice(0, 80);
}
function record(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function iso(value: unknown): value is string {
  return typeof value === "string" && /^\d{4}-\d{2}-\d{2}T/.test(value) && Number.isFinite(Date.parse(value));
}
function validEntry(value: unknown): value is QueueEntry {
  if (!record(value)) return false;
  return typeof value.id === "string" && /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i.test(value.id)
    && typeof value.endpointId === "string" && /^[0-9a-f]{8}$/.test(value.endpointId)
    && Number.isSafeInteger(value.pid) && (value.pid as number) > 0
    && typeof value.sessionId === "string" && value.sessionId.length > 0
    && (value.name === undefined || typeof value.name === "string")
    && typeof value.title === "string" && value.title.length <= 80
    && (value.state === "active" || value.state === "waiting")
    && iso(value.enqueuedAt) && (value.grantedAt === undefined || iso(value.grantedAt))
    && (value.holdExpiresAt === null || (typeof value.holdExpiresAt === "number" && Number.isFinite(value.holdExpiresAt)));
}

/** Never follow a file symlink, and refuse nonregular or foreign-owned files. */
function readPrivate(path: string): { text: string; ino: number; mtimeMs: number } {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const st = fstatSync(fd);
    if (!st.isFile() || (process.getuid && st.uid !== process.getuid())) {
      throw new Error(`unsafe queue file: ${path}`);
    }
    return { text: readFileSync(fd, "utf8"), ino: st.ino, mtimeMs: st.mtimeMs };
  } finally { closeSync(fd); }
}
function readQueue(path: string, repo: string): QueueFile {
  const empty: QueueFile = { v: 1, repo, entries: [] };
  let text: string;
  try { text = readPrivate(path).text; }
  catch (err) {
    if ((err as NodeJS.ErrnoException).code === "ENOENT") return empty;
    throw err;
  }
  let value: unknown;
  try { value = JSON.parse(text); } catch { return empty; }
  if (!record(value) || value.v !== 1 || value.repo !== repo || !Array.isArray(value.entries)) return empty;
  return { v: 1, repo, entries: value.entries.filter(validEntry).map((entry) => ({
    id: entry.id, endpointId: entry.endpointId, pid: entry.pid, sessionId: entry.sessionId,
    ...(entry.name === undefined ? {} : { name: entry.name }), title: queueTitle(entry.title),
    state: entry.state, enqueuedAt: entry.enqueuedAt,
    ...(entry.grantedAt === undefined ? {} : { grantedAt: entry.grantedAt }), holdExpiresAt: entry.holdExpiresAt,
  })) };
}

/** In-place invariant repair. File order is FIFO; any extra active entries become waiters. */
export function normalizeQueue(file: QueueFile, now: number, idleMs: number): QueueEntry | undefined {
  const seen = new Set<string>();
  let holder: QueueEntry | undefined;
  const waiting: QueueEntry[] = [];
  for (const entry of file.entries) {
    if (!validEntry(entry) || isGone(entry.pid)) continue;
    const identity = `${entry.endpointId}:${entry.pid}`;
    if (seen.has(identity)) continue;
    seen.add(identity);
    if (entry.state === "active" && !holder) {
      if (entry.holdExpiresAt !== null && entry.holdExpiresAt <= now) continue;
      holder = entry;
    } else {
      entry.state = "waiting";
      entry.holdExpiresAt = null;
      delete entry.grantedAt;
      waiting.push(entry);
    }
  }
  file.entries = holder ? [holder, ...waiting] : waiting;
  if (holder || !file.entries.length) return undefined;
  const promoted = file.entries[0]!;
  promoted.state = "active";
  promoted.grantedAt = new Date(now).toISOString();
  promoted.holdExpiresAt = now + idleMs;
  return promoted;
}

function unlock(path: string, token: string): void {
  try {
    const snapshot = readPrivate(path);
    const value: unknown = JSON.parse(snapshot.text);
    if (record(value) && value.token === token && lstatSync(path).ino === snapshot.ino) unlinkSync(path);
  } catch { /* Missing or replaced: never unlink someone else's lock. */ }
}
function reclaimLock(path: string): void {
  try {
    const snapshot = readPrivate(path);
    let dead = false;
    try {
      const value: unknown = JSON.parse(snapshot.text);
      dead = record(value) && Number.isSafeInteger(value.pid) && (value.pid as number) > 0 && isGone(value.pid as number);
    } catch { /* An incomplete/malformed lock is reclaimable only after its age deadline. */ }
    if ((dead || Date.now() - snapshot.mtimeMs > 10_000) && lstatSync(path).ino === snapshot.ino) unlinkSync(path);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
  }
}

export function createQueueStore(options: QueueStoreOptions): QueueStore {
  const key = queueKey(options.repo);
  const dir = join(options.busDir, "queue");
  const path = join(dir, `${key}.json`);
  const lock = join(dir, `${key}.lock`);
  const now = options.now ?? Date.now;
  const idleMs = options.idleMs ?? 600_000;
  const prepare = (): void => { ensurePrivateDir(options.busDir); ensurePrivateDir(dir); };
  async function mutate(fn: (file: QueueFile, now: number) => void): Promise<QueueMutation> {
    prepare();
    const token = randomUUID();
    const deadline = Date.now() + (options.lockTimeoutMs ?? 2000);
    for (;;) {
      let fd: number;
      try { fd = openSync(lock, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600); }
      catch (err) {
        if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err;
        reclaimLock(lock);
        if (Date.now() >= deadline) throw new Error(`work queue lock timed out: ${lock}`);
        await new Promise<void>((resolve) => setTimeout(resolve, 10 + Math.floor(Math.random() * 25)));
        continue;
      }
      // No await from creation of the lock through unlink: all critical-section I/O is synchronous.
      let tmp: string | undefined;
      try {
        try { writeFileSync(fd, JSON.stringify({ pid: process.pid, token })); }
        finally { closeSync(fd); }
        const time = now();
        const file = readQueue(path, options.repo);
        const prior = file.entries.find((entry) => entry.state === "active")?.id;
        normalizeQueue(file, time, idleMs);
        const result: unknown = fn(file, time);
        if (result && typeof (result as { then?: unknown }).then === "function") {
          throw new Error("queue mutations must be synchronous");
        }
        normalizeQueue(file, time, idleMs);
        tmp = join(dir, `${key}.${token}.tmp`);
        const out = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
        try { writeFileSync(out, JSON.stringify(file)); } finally { closeSync(out); }
        renameSync(tmp, path);
        tmp = undefined;
        const holder = file.entries[0];
        return { file, ...(holder?.state === "active" && holder.id !== prior ? { promoted: holder } : {}) };
      } finally {
        if (tmp) { try { unlinkSync(tmp); } catch { /* best effort */ } }
        unlock(lock, token);
      }
    }
  }
  const store: QueueStore = {
    key, repo: options.repo, path,
    read() { prepare(); return readQueue(path, options.repo); },
    mutate,
    async enqueue(owner) {
      let id: string = "";
      const result = await mutate((file, time) => {
        const existing = file.entries.find((entry) => entry.endpointId === owner.endpointId && entry.pid === owner.pid);
        if (existing) { id = existing.id; return; }
        const entry: QueueEntry = { ...owner, title: queueTitle(owner.title), id: randomUUID(),
          state: "waiting", enqueuedAt: new Date(time).toISOString(), holdExpiresAt: null };
        if (!validEntry(entry)) throw new Error("invalid queue owner");
        id = entry.id;
        file.entries.push(entry);
      });
      const position = result.file.entries.findIndex((entry) => entry.id === id) + 1;
      return { ...result, entry: result.file.entries[position - 1]!, position, holder: result.file.entries[0] };
    },
    removeEntry(endpointId, pid) {
      return mutate((file) => { file.entries = file.entries.filter((entry) => entry.endpointId !== endpointId || entry.pid !== pid); });
    },
    removeIfSameId(id) { return mutate((file) => { file.entries = file.entries.filter((entry) => entry.id !== id); }); },
    setHoldExpiry(id, expiry) {
      return mutate((file) => {
        const holder = file.entries.find((entry) => entry.id === id && entry.state === "active");
        if (holder) holder.holdExpiresAt = expiry;
      });
    },
  };
  return store;
}

export interface ProbeHolderOptions { timeoutMs?: number; socketPath?: SocketPathOptions }
export async function probeHolder(busDir: string, entry: QueueEntry, options: ProbeHolderOptions = {}): Promise<"alive" | "dead" | "unknown"> {
  const socket = listSockets(busDir, options.socketPath).find((candidate) => candidate.id === entry.endpointId && candidate.pid === entry.pid);
  if (!socket) return "dead";
  try {
    const peer = await helloProbe(socket.path, options.timeoutMs);
    return peer.id === entry.endpointId && peer.pid === entry.pid ? "alive" : "unknown";
  } catch (err) { return isStaleError(err) ? "dead" : "unknown"; }
}
export interface NudgePromotedOptions extends ProbeHolderOptions {
  ownEndpointId?: string;
  onOwn?: (entry: QueueEntry) => void | Promise<void>;
  maxAttempts?: number;
}
/** Best effort. A proven stale owner is removed by id before trying the next promotion. */
export async function nudgePromoted(store: QueueStore, initial: QueueEntry | undefined, options: NudgePromotedOptions = {}): Promise<QueueEntry | undefined> {
  let entry = initial;
  for (let attempt = 0; entry && attempt < (options.maxAttempts ?? 16); attempt++) {
    if (entry.endpointId === options.ownEndpointId) { await options.onOwn?.(entry); return entry; }
    const socket = listSockets(dirname(dirname(store.path)), options.socketPath)
      .find((candidate) => candidate.id === entry!.endpointId && candidate.pid === entry!.pid);
    if (socket) {
      try { await sendQueueNudge(socket.path, store.key, options.timeoutMs); return entry; }
      catch (err) { if (!isStaleError(err)) return entry; }
    }
    const result = await store.removeIfSameId(entry.id);
    entry = result.promoted;
  }
  return entry;
}
