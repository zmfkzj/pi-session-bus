/** Repository work queue. Atomic JSON snapshots are authoritative; sockets only nudge. */
import { createHash, randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { helloProbe, isGone, isStaleError, sendQueueNudge } from "./endpoint.ts";
import { publishGeneration, readPrivate, readSnapshot, STALE_LOCK_MS, voidGeneration, withGeneration, type HeldGeneration } from "./queue-log.ts";
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
  /** Lock acquisition deadline, measured by the monotonic clock (never the injectable queue clock). */
  lockTimeoutMs?: number;
}
export interface QueueStore {
  readonly key: string;
  readonly repo: string;
  /** Directory of the queue's lock generations and snapshots (`<bus dir>/queue/<key>.v2`). */
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

function parseQueue(text: string | undefined, repo: string): QueueFile {
  const empty: QueueFile = { v: 1, repo, entries: [] };
  if (text === undefined) return empty;
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
function readFileText(path: string): string | undefined {
  try { return readPrivate(path).text; }
  catch (err) { if ((err as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw err; }
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

/** Sessions still on queue protocol v1 (`<key>.lock` and `<key>.json`), which cannot see the v2 queue. */
export class LegacyQueueError extends Error {
  readonly pids: number[];
  constructor(pids: number[], path: string) {
    super(`an older session-bus (pid ${pids.join(", ")}) holds the previous work queue ${path} and cannot see this one; reload or quit that session`);
    this.name = "LegacyQueueError";
    this.pids = pids;
  }
}
/**
 * Live foreign pids that hold the v1 lock right now or the v1 turn. v1 files are only read, never changed: their
 * waiters cannot take a turn without a v1 holder, and dead or expired holders are ignored.
 */
function legacyPids(lockPath: string, queuePath: string, repo: string, time: number): number[] {
  const pids = new Set<number>();
  const live = (pid: number): boolean => pid !== process.pid && !isGone(pid);
  try {
    const lock = readPrivate(lockPath);
    const value: unknown = JSON.parse(lock.text);
    if (record(value) && Number.isSafeInteger(value.pid) && (value.pid as number) > 0
      && Date.now() - lock.mtimeMs <= STALE_LOCK_MS && live(value.pid as number)) pids.add(value.pid as number);
  } catch { /* none, malformed or not a regular file */ }
  try {
    for (const entry of parseQueue(readFileText(queuePath), repo).entries) {
      if (entry.state === "active" && (entry.holdExpiresAt === null || entry.holdExpiresAt > time) && live(entry.pid)) pids.add(entry.pid);
    }
  } catch { /* unreadable: nothing to honor */ }
  return [...pids];
}

export function createQueueStore(options: QueueStoreOptions): QueueStore {
  const key = queueKey(options.repo);
  const dir = join(options.busDir, "queue");
  const path = join(dir, `${key}.v2`);
  const legacyQueue = join(dir, `${key}.json`);
  const legacyLock = join(dir, `${key}.lock`);
  const now = options.now ?? Date.now;
  const idleMs = options.idleMs ?? 600_000;
  const prepare = (): void => { ensurePrivateDir(options.busDir); ensurePrivateDir(dir); ensurePrivateDir(path); };
  /** The critical section: synchronous from the lock through publication of the generation's snapshot. */
  function section(held: HeldGeneration, fn: (file: QueueFile, now: number) => void): QueueMutation {
    let base: string | undefined;
    let readable = false;
    let published = false;
    try {
      base = readSnapshot(held.dir, held.gen);
      readable = true;
      const time = now();
      const legacy = legacyPids(legacyLock, legacyQueue, options.repo, time);
      if (legacy.length) throw new LegacyQueueError(legacy, legacyQueue);
      const file = parseQueue(base, options.repo);
      const prior = file.entries.find((entry) => entry.state === "active")?.id;
      normalizeQueue(file, time, idleMs);
      const result: unknown = fn(file, time);
      if (result && typeof (result as { then?: unknown }).then === "function") {
        throw new Error("queue mutations must be synchronous");
      }
      normalizeQueue(file, time, idleMs);
      publishGeneration(held, JSON.stringify(file));
      published = true;
      const holder = file.entries[0];
      return { file, ...(holder?.state === "active" && holder.id !== prior ? { promoted: holder } : {}) };
    } finally {
      // Release unchanged: republish the base, or without a readable base void the generation.
      if (!published) {
        try {
          if (readable) publishGeneration(held, base ?? JSON.stringify({ v: 1, repo: options.repo, entries: [] }));
          else voidGeneration(held);
        } catch { /* fenced or I/O failure: the generation is reclaimed after its lease */ }
      }
    }
  }
  async function mutate(fn: (file: QueueFile, now: number) => void): Promise<QueueMutation> {
    prepare();
    return withGeneration(path, options.lockTimeoutMs ?? 2000, (held) => section(held, fn));
  }
  const store: QueueStore = {
    key, repo: options.repo, path,
    read() { prepare(); return parseQueue(readSnapshot(path), options.repo); },
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
