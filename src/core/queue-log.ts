/**
 * Queue lock protocol v2: numbered lock generations and fenced snapshots, in `queue/<key>.v2/`.
 *
 * Names: `L<g>` lock of generation g (`{v:2,pid,token,after,mono,boot}`), `S<g>.json` the snapshot that releases it,
 * `V<g>` a void marker that releases a generation without an update, `T<g>.…tmp` private temporary files.
 *
 * Invariants and why they hold:
 * 1. A lock name is removed only by collection, never to take over or to give up. Taking over from a committed, void,
 *    dead or stale holder of the newest generation g means creating `L<g+1>`; `link` gives each name to exactly one
 *    process. A contender that loses gives up by writing `V<g>` and keeps its lock, so lock names never have holes.
 * 2. Collection is bottom-up by name: `L<n>` is removed only while `L<n-1>` does not exist, so a lock never outlives
 *    an older one. Generation n is collected only when n <= a - KEEP_GENERATIONS for a published generation a (`S<a>`
 *    exists) whose lock is older than COLLECT_AGE_MS, and the newest snapshot is never collected.
 * 3. An acquisition starts its clock before its directory scan and enters only if, after creating `L<m+1>`, the name
 *    `L<m+2>` does not exist and less than ACQUIRE_WINDOW_MS has passed. Let N be the newest lock at the start. An
 *    anchor a >= N + KEEP_GENERATIONS was linked after the start; its holder stamped it at most ACQUIRE_WINDOW_MS
 *    before its own successful check, which came after the link. So the anchor is COLLECT_AGE_MS old only when more
 *    than COLLECT_AGE_MS - ACQUIRE_WINDOW_MS = ACQUIRE_WINDOW_MS has passed since the start. Hence, within the window,
 *    L(N) is not collected and the scan returns N or newer (POSIX returns every entry present for the whole
 *    enumeration; entries created or removed during it may be missed, which only makes a scan stale), and neither
 *    `L<m+1>` nor `L<m+2>` can have been collected: a successful `link` created a new name, and an absent `L<m+2>`
 *    never existed. A stale scan or a longer pause fails `link` (EEXIST) or the window, and the contender gives up.
 * 4. A holder publishes `S<g>`, then reads its successor `L<g+1>`, then checks that `L<g>` is still its own. By 2, a
 *    successor cannot have been collected while `L<g>` existed, so an absent successor did not exist before the
 *    snapshot; a successor that reclaimed the generation instead of seeing the snapshot (after `reclaimed`), or a
 *    collected own lock, reports the update as failed (QueueFencedError) instead of applied. This uses no clock.
 *
 * Time: every rule that safety depends on (3, and the anchor age in 2) measures one monotonic clock, the system-wide
 * CLOCK_MONOTONIC of this boot (Node's `process.hrtime`), stamped into each lock as `mono` with the boot id `boot`,
 * so changes of the wall clock in either direction cannot shorten a window or age an anchor early. A lock of another
 * boot was linked before every live process started and may count as old. The lease of rule 1 uses the same stamps;
 * it decides only when a silent holder is fenced, never whether an update is reported correctly. File mtimes and
 * the wall clock are used only for locks without a stamp (liveness) and for the v1 lock of older versions.
 * Assumption: all processes that share the bus directory run on one host and boot, in one time namespace.
 */
import { randomUUID } from "node:crypto";
import {
  closeSync, constants, fstatSync, linkSync, lstatSync, openSync, readdirSync, readFileSync, renameSync,
  unlinkSync, writeFileSync,
} from "node:fs";
import { join } from "node:path";
import { isGone } from "./endpoint.ts";

/** A lock whose holder neither published nor died is reclaimed after this lease. */
export const STALE_LOCK_MS = 10_000;
/** Generations kept behind the newest old snapshot. */
export const KEEP_GENERATIONS = 16;
/** Age of the anchor generation's lock before generations behind it are collected. */
export const COLLECT_AGE_MS = 60_000;
/** An acquisition slower than this from its first scan to its checks gives up its generation and starts over. */
export const ACQUIRE_WINDOW_MS = COLLECT_AGE_MS / 2;
function bootId(): string | undefined {
  try { return readFileSync("/proc/sys/kernel/random/boot_id", "utf8").trim() || undefined; } catch { return undefined; }
}
/**
 * Clocks of the protocol. `mono` (milliseconds of CLOCK_MONOTONIC, shared by all processes of this boot) and `boot`
 * (undefined where the platform has no boot id) decide safety; `wall` only liveness fallbacks. Tests replace them to
 * simulate pauses, wall clock changes and reboots.
 */
export const clock = {
  wall: (): number => Date.now(),
  mono: (): number => Number(process.hrtime.bigint() / 1000n) / 1000,
  boot: bootId(),
};
const WIDTH = 15;
const LOCK = /^L(\d{15})$/;
const SNAPSHOT = /^S(\d{15})\.json$/;
const VOID = /^V(\d{15})$/;
const TMP = /^T(\d{15})\.[0-9a-f-]{36}\.[ls]\.tmp$/;

const pad = (gen: number): string => String(gen).padStart(WIDTH, "0");
export const lockName = (gen: number): string => `L${pad(gen)}`;
export const snapshotName = (gen: number): string => `S${pad(gen)}.json`;
export const voidName = (gen: number): string => `V${pad(gen)}`;
const tmpName = (gen: number, token: string, kind: "l" | "s"): string => `T${pad(gen)}.${token}.${kind}.tmp`;

/** The update may not have been applied: this holder's generation was reclaimed or collected before it finished. */
export class QueueFencedError extends Error {
  constructor(dir: string, gen: number) {
    super(`work queue update may not have been applied: generation ${gen} in ${dir} was reclaimed while this process was paused`);
    this.name = "QueueFencedError";
  }
}

export interface LockInfo {
  v: 2; pid: number; token: string; after: "committed" | "reclaimed";
  /** `clock.mono()` when the lock was written, and the boot it belongs to (absent where unknown). */
  mono?: number; boot?: string;
}
export interface HeldGeneration { readonly dir: string; readonly gen: number; readonly token: string }

/** Never follow a file symlink, and refuse nonregular or foreign-owned files. */
export function readPrivate(path: string): { text: string; ino: number; mtimeMs: number } {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const st = fstatSync(fd);
    if (!st.isFile() || (process.getuid && st.uid !== process.getuid())) {
      throw new Error(`unsafe queue file: ${path}`);
    }
    return { text: readFileSync(fd, "utf8"), ino: st.ino, mtimeMs: st.mtimeMs };
  } finally { closeSync(fd); }
}

const missing = (err: unknown): boolean => (err as NodeJS.ErrnoException).code === "ENOENT";
function exists(path: string): boolean {
  try { lstatSync(path); return true; } catch (err) { if (missing(err)) return false; throw err; }
}
function remove(path: string): void {
  try { unlinkSync(path); } catch (err) { if (!missing(err)) throw err; }
}

interface Scan { lock: number; snapshot: number; locks: number[]; snapshots: number[]; voids: number[]; tmps: string[] }
function scan(dir: string): Scan {
  const result: Scan = { lock: 0, snapshot: 0, locks: [], snapshots: [], voids: [], tmps: [] };
  for (const name of readdirSync(dir)) {
    let match = LOCK.exec(name);
    if (match) { const gen = Number(match[1]); result.locks.push(gen); result.lock = Math.max(result.lock, gen); continue; }
    match = SNAPSHOT.exec(name);
    if (match) { const gen = Number(match[1]); result.snapshots.push(gen); result.snapshot = Math.max(result.snapshot, gen); continue; }
    match = VOID.exec(name);
    if (match) { result.voids.push(Number(match[1])); continue; }
    if (TMP.test(name)) result.tmps.push(name);
  }
  return result;
}

function validLock(value: unknown): value is LockInfo {
  const v = value as Partial<LockInfo> | null;
  return typeof v === "object" && v !== null && v.v === 2 && Number.isSafeInteger(v.pid) && v.pid! > 0
    && typeof v.token === "string" && (v.after === "committed" || v.after === "reclaimed")
    && (v.mono === undefined || (Number.isFinite(v.mono) && v.mono >= 0)) && (v.boot === undefined || typeof v.boot === "string");
}
/** Whether a stamped lock belongs to an earlier boot: monotonic clocks restart, and its holder is gone. */
function otherBoot(info: LockInfo & { mono: number }): boolean {
  if (info.boot !== undefined && clock.boot !== undefined) return info.boot !== clock.boot;
  return info.mono > clock.mono(); // Without boot ids: a stamp ahead of this boot's clock.
}
/**
 * Milliseconds since the lock was stamped by the monotonic clock of this boot, or undefined when that is unknown
 * (unstamped, or another boot): callers then treat it as young, which only delays collection.
 */
export function stampAge(info: LockInfo | null | undefined): number | undefined {
  if (!info || info.mono === undefined || otherBoot(info as LockInfo & { mono: number })) return undefined;
  return Math.max(0, clock.mono() - info.mono);
}
/** The lock of `gen`, `null` when it is malformed, or undefined when it does not exist. */
export function readLock(dir: string, gen: number): { info: LockInfo | null; mtimeMs: number } | undefined {
  let raw: { text: string; mtimeMs: number };
  try { raw = readPrivate(join(dir, lockName(gen))); } catch (err) { if (missing(err)) return undefined; throw err; }
  let value: unknown;
  try { value = JSON.parse(raw.text); } catch { value = undefined; }
  return { info: validLock(value) ? value : null, mtimeMs: raw.mtimeMs };
}

/**
 * Why the holder of `gen` can be succeeded, or undefined while it may still be working. Within this process a
 * critical section never awaits, so an unreleased lock of this pid seen between attempts was abandoned.
 */
export function takeover(dir: string, gen: number): LockInfo["after"] | undefined {
  if (gen === 0 || exists(join(dir, snapshotName(gen))) || exists(join(dir, voidName(gen)))) return "committed";
  const lock = readLock(dir, gen);
  if (!lock) return undefined; // Collected meanwhile: the next scan sees newer generations.
  if (lock.info && (lock.info.pid === process.pid || isGone(lock.info.pid))) return "reclaimed";
  // The lease: by the stamp, so wall clock changes neither expire nor extend it; a lock of an earlier boot is over.
  if (lock.info?.mono !== undefined) {
    if (otherBoot(lock.info as LockInfo & { mono: number })) return "reclaimed";
    return clock.mono() - lock.info.mono > STALE_LOCK_MS ? "reclaimed" : undefined;
  }
  return clock.wall() - lock.mtimeMs > STALE_LOCK_MS ? "reclaimed" : undefined; // unstamped or malformed lock
}

/** Creates the complete lock file of `gen`, or returns false when the name is taken (or the attempt was collected). */
function createLock(dir: string, gen: number, info: LockInfo): boolean {
  const tmp = join(dir, tmpName(gen, info.token, "l"));
  const fd = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try {
    try { writeFileSync(fd, JSON.stringify(info)); } finally { closeSync(fd); }
    linkSync(tmp, join(dir, lockName(gen)));
    return true;
  } catch (err) {
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "EEXIST" || code === "ENOENT") return false;
    throw err;
  } finally { try { unlinkSync(tmp); } catch { /* best effort */ } }
}

/** Releases a held generation without an update (its lock stays, so lock names never have holes). */
export function voidGeneration(held: HeldGeneration): void {
  try { writeFileSync(join(held.dir, voidName(held.gen)), held.token, { mode: 0o600, flag: "wx" }); }
  catch (err) { if ((err as NodeJS.ErrnoException).code !== "EEXIST") throw err; }
}

/**
 * Waits for the newest generation to end, takes the next one and runs `section` synchronously right away. The
 * section must publish its snapshot or void the generation before it returns: no await may separate the lock from
 * its release, because other contenders of this process treat an unreleased lock of this pid as abandoned.
 */
export async function withGeneration<T>(dir: string, timeoutMs: number, section: (held: HeldGeneration) => T): Promise<T> {
  const token = randomUUID();
  const deadline = clock.mono() + timeoutMs;
  for (let attempt = 0; ; attempt++) {
    const started = clock.mono();
    const newest = scan(dir).lock;
    const after = takeover(dir, newest);
    if (after) {
      const held: HeldGeneration = { dir, gen: newest + 1, token };
      if (createLock(dir, held.gen, { v: 2, pid: process.pid, token, after, mono: clock.mono(), ...(clock.boot ? { boot: clock.boot } : {}) })) {
        // Invariant 3: a newer generation, or a collected name created again after a pause, is detected here.
        if (!exists(join(dir, lockName(held.gen + 1))) && clock.mono() - started < ACQUIRE_WINDOW_MS) return section(held);
        voidGeneration(held);
      }
      if (attempt < 64) continue;
    }
    if (clock.mono() >= deadline) throw new Error(`work queue lock timed out: ${join(dir, lockName(newest))}`);
    await new Promise<void>((resolve) => setTimeout(resolve, 10 + Math.floor(Math.random() * 25)));
  }
}

/** Raw text of the newest snapshot, below `below` when given; undefined when there is none. */
export function readSnapshot(dir: string, below = Number.MAX_SAFE_INTEGER): string | undefined {
  for (let attempt = 0; attempt < 8; attempt++) {
    const gens = scan(dir).snapshots.filter((gen) => gen < below);
    if (!gens.length) return undefined;
    try { return readPrivate(join(dir, snapshotName(Math.max(...gens)))).text; }
    catch (err) { if (!missing(err)) throw err; } // Collected behind a newer snapshot: scan again.
  }
  throw new Error(`work queue snapshots in ${dir} keep changing`);
}

/** Publishes the snapshot of a held generation, which releases it. Synchronous: call it in the critical section. */
export function publishGeneration(held: HeldGeneration, text: string): void {
  const own = (): boolean => readLock(held.dir, held.gen)?.info?.token === held.token;
  if (exists(join(held.dir, lockName(held.gen + 1))) || !own()) throw new QueueFencedError(held.dir, held.gen);
  const tmp = join(held.dir, tmpName(held.gen, held.token, "s"));
  const out = openSync(tmp, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try {
    try { writeFileSync(out, text); } finally { closeSync(out); }
    renameSync(tmp, join(held.dir, snapshotName(held.gen)));
  } catch (err) {
    try { unlinkSync(tmp); } catch { /* best effort */ }
    // Collected while paused: the generation is far behind the newest, so its successor exists.
    if (missing(err)) throw new QueueFencedError(held.dir, held.gen);
    throw err;
  }
  // Invariant 4: successor first, then the own lock. The order matters: see the module comment.
  const successor = readLock(held.dir, held.gen + 1);
  if ((successor && successor.info?.after !== "committed") || !own()) throw new QueueFencedError(held.dir, held.gen);
  collectGenerations(held.dir, held.gen);
}

/** Collects old generations bottom-up (invariant 2) and stray temporary files behind them. Best effort; never throws. */
export function collectGenerations(dir: string, gen: number): void {
  try {
    const found = scan(dir);
    let anchor = 0;
    for (const old of [...found.snapshots].sort((a, b) => b - a)) {
      if (old > gen) continue;
      // Rule 2 by the monotonic stamp of the anchor's lock (the same clock as the acquisition window).
      if ((stampAge(readLock(dir, old)?.info) ?? 0) > COLLECT_AGE_MS) { anchor = old; break; }
    }
    const limit = Math.min(anchor, found.snapshot) - KEEP_GENERATIONS;
    const gens = [...new Set([...found.locks, ...found.snapshots, ...found.voids])].filter((old) => old <= limit).sort((a, b) => a - b);
    for (const old of gens) {
      if (exists(join(dir, lockName(old - 1)))) break; // An older lock remains: removing this one would make a hole.
      remove(join(dir, voidName(old)));
      remove(join(dir, snapshotName(old)));
      remove(join(dir, lockName(old)));
    }
    // A temporary file of a generation this far behind belongs to a contender or holder that will give up anyway.
    for (const name of found.tmps) if (Number(TMP.exec(name)![1]) <= limit) remove(join(dir, name));
  } catch { /* Collection is best effort and never fails a committed update. */ }
}
