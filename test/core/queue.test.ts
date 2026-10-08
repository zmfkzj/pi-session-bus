import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs, { existsSync, lstatSync, readdirSync, readFileSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createQueueStore, LegacyQueueError, normalizeQueue, nudgePromoted, probeHolder, queueKey, queueTitle, type QueueEntry, type QueueFile, type QueueStore } from "../../src/core/queue.ts";
import { ACQUIRE_WINDOW_MS, clock as logClock, COLLECT_AGE_MS, collectGenerations, KEEP_GENERATIONS, lockName, QueueFencedError, snapshotName, STALE_LOCK_MS, voidName } from "../../src/core/queue-log.ts";
import { createEndpoint, type Endpoint } from "../../src/core/endpoint.ts";
import { socketFileName } from "../../src/core/registry.ts";
import { findDeadPid, holdQueueLock, listenHello, listenSilent, listenWith, makeStaleSocket, makeTempDir, peerInfo, removeTempDir, writeQueueSnapshot, type HeldQueueLock, type TestServer } from "./helpers.ts";

let tmp: string;
let store: QueueStore;
let clock: number;
let servers: TestServer[];
let endpoints: Endpoint[];
const owner = (n: number) => ({ endpointId: n.toString(16).padStart(8, "0"), pid: process.pid, sessionId: `session-${n}`, title: `task ${n}` });
/** The protocol's clocks, restored after each test; `elapse` simulates real time passing (monotonic and wall). */
const realClock = { ...logClock };
let monoOffset = 0;
let wallOffset = 0;
const elapse = (ms: number) => { monoOffset += ms; wallOffset += ms; };
/** The wall clock is set by `ms` (forward or backward); the monotonic clock is not. */
const setWallClock = (ms: number) => { wallOffset += ms; };
beforeEach(() => {
  tmp = makeTempDir("sb-q"); clock = Date.now(); servers = []; endpoints = [];
  store = createQueueStore({ busDir: tmp, repo: "/repo/worktree", idleMs: 600_000, now: () => clock });
  monoOffset = wallOffset = 0;
  logClock.mono = () => realClock.mono() + monoOffset;
  logClock.wall = () => realClock.wall() + wallOffset;
  logClock.boot = realClock.boot;
});
afterEach(async () => {
  Object.assign(logClock, realClock);
  await Promise.all(endpoints.map((endpoint) => endpoint.stop()));
  await Promise.all(servers.map((server) => server.close()));
  removeTempDir(tmp);
});
const lockFile = (gen: number) => JSON.parse(readFileSync(join(store.path, lockName(gen)), "utf8")) as { pid: number; token: string; after: string; mono?: number; boot?: string };
const newestSnapshot = (): QueueFile => JSON.parse(readFileSync(join(store.path, snapshotName(Math.max(...readdirSync(store.path)
  .filter((name) => /^S\d{15}\.json$/.test(name)).map((name) => Number(name.slice(1, 16)))))), "utf8")) as QueueFile;
/**
 * Runs `before` once, synchronously, right before the first call of `fs[name]` by any module whose path argument
 * matches `when` (an interleaving: other processes act while this one is paused there). `before` may return a
 * replacement result (used to return a directory listing taken before its changes).
 */
function interceptOnce(name: "linkSync" | "renameSync" | "lstatSync" | "readdirSync" | "openSync", before: () => unknown, when: (path: string) => boolean = () => true): () => void {
  const real = fs[name] as (...args: unknown[]) => unknown;
  let fired = false;
  (fs as unknown as Record<string, unknown>)[name] = (...args: unknown[]) => {
    const path = String(args[name === "linkSync" ? 1 : 0]);
    if (!fired && when(path)) { fired = true; const replaced = before(); if (replaced !== undefined) return replaced; }
    return real(...args);
  };
  syncBuiltinESMExports();
  return () => { (fs as unknown as Record<string, unknown>)[name] = real; syncBuiltinESMExports(); };
}
const newestLock = (): number => Math.max(0, ...readdirSync(store.path).filter((name) => /^L\d{15}$/.test(name)).map((name) => Number(name.slice(1))));
/** Sets a file's mtime `ms` into the past (what a wall clock jump, not elapsed time, does to file ages). */
const age = (path: string, ms: number) => { const past = new Date(Date.now() - ms); utimesSync(path, past, past); };
/**
 * Other processes, while this one is paused: the generation after `from` reclaims it (or `after` says how), then
 * generations up to `from + count` commit `content`, time passes (`pauseMs`, by default enough for their locks to
 * be old), and the newest collects.
 */
function othersAdvance(from: number, count: number, content: QueueFile, after: "committed" | "reclaimed" = "reclaimed",
  pauseMs = COLLECT_AGE_MS + 60_000): number {
  for (let k = 1; k <= count; k++) holdQueueLock(tmp, store.repo, process.ppid, k === 1 ? after : "committed").publish(content);
  elapse(pauseMs);
  collectGenerations(store.path, from + count);
  return from + count;
}
const entryOf = (n: number, pid = process.pid): QueueEntry => ({ ...owner(n), pid, id: crypto.randomUUID(), state: "waiting",
  enqueuedAt: new Date().toISOString(), holdExpiresAt: null });
const invariants = (file: QueueFile): void => {
  assert.ok(file.entries.filter((entry) => entry.state === "active").length <= 1);
  if (file.entries.length) assert.equal(file.entries[0]!.state, "active");
  assert.ok(file.entries.slice(1).every((entry) => entry.state === "waiting"));
  assert.equal(new Set(file.entries.map((entry) => `${entry.endpointId}:${entry.pid}`)).size, file.entries.length);
};

describe("repository queue", () => {
  it("hashes repository paths and flattens only the first prompt line", () => {
    assert.match(queueKey("/repo"), /^[0-9a-f]{16}$/);
    assert.notEqual(queueKey("/repo"), queueKey("/repo/submodule"));
    assert.equal(queueTitle(" hi\tthere\nsecond line"), "hi there");
    assert.equal(queueTitle("a".repeat(100)).length, 80);
  });
  it("acquires and releases in FIFO order with one entry per endpoint and pid", async () => {
    const first = await store.enqueue(owner(1));
    const second = await store.enqueue(owner(2));
    const third = await store.enqueue(owner(3));
    assert.equal(first.entry.state, "active");
    assert.equal(first.promoted?.id, first.entry.id);
    assert.equal(second.position, 2); assert.equal(third.position, 3);
    assert.equal((await store.enqueue(owner(2))).entry.id, second.entry.id);
    const release = await store.removeEntry(owner(1).endpointId, process.pid);
    assert.equal(release.promoted?.id, second.entry.id);
    assert.deepEqual(release.file.entries.map((entry) => entry.endpointId), [owner(2).endpointId, owner(3).endpointId]);
    invariants(release.file);
    assert.equal(lstatSync(store.path).mode & 0o777, 0o700);
    assert.equal(lstatSync(join(tmp, "queue")).mode & 0o777, 0o700);
    // Every lock generation is released by its snapshot, and no temporary file remains.
    const names = readdirSync(store.path);
    assert.ok(names.every((name) => /^L\d{15}$/.test(name) ? names.includes(`S${name.slice(1)}.json`) || names.includes(`V${name.slice(1)}`) : /^[SV]\d{15}(\.json)?$/.test(name)), names.join());
    assert.ok(names.filter((name) => name.startsWith("S")).every((name) => (lstatSync(join(store.path, name)).mode & 0o777) === 0o600));
  });
  it("keeps one active entry and loses no entries under concurrent child-process mutations", async () => {
    store.read();
    const workers = Array.from({ length: 5 }, (_, i) => new Promise<void>((resolve, reject) => {
      const child = spawn(process.execPath, [new URL("./queue-worker.ts", import.meta.url).pathname, tmp, store.repo,
        i.toString(16).padStart(4, "0"), String(process.pid)], { stdio: ["ignore", "ignore", "pipe"] });
      let stderr = "";
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      child.once("error", reject);
      child.once("exit", (code) => code === 0 ? resolve() : reject(new Error(`worker exit ${code}: ${stderr}`)));
    }));
    const observer = setInterval(() => invariants(store.read()), 2);
    try { await Promise.all(workers); } finally { clearInterval(observer); }
    const file = store.read(); invariants(file);
    assert.equal(file.entries.length, 60);
  });
  it("loses no reported update when contending processes are killed at random points, including inside the lock", async () => {
    store.read();
    const reported: string[] = [];
    const kills: Promise<void>[] = [];
    const workers = Array.from({ length: 8 }, (_, i) => new Promise<void>((resolve, reject) => {
      const child = spawn(process.execPath, [new URL("./queue-worker.ts", import.meta.url).pathname, tmp, store.repo,
        (i + 0x10).toString(16).padStart(4, "0"), String(process.pid), "10"], { stdio: ["ignore", "pipe", "pipe"] });
      let stdout = ""; let stderr = "";
      child.stdout.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        reported.push(...stdout.split("\n").filter((line) => /^[0-9a-f-]{36}$/.test(line)));
        if (code === 0 || signal === "SIGKILL") resolve(); else reject(new Error(`worker exit ${code}: ${stderr}`));
      });
      // Half of the workers die abruptly, often while they hold a lock generation.
      if (i % 2) kills.push(new Promise((done) => setTimeout(() => { child.kill("SIGKILL"); done(); }, 150 + Math.random() * 400)));
    }));
    const observer = setInterval(() => invariants(store.read()), 2);
    try { await Promise.all([...workers, ...kills]); } finally { clearInterval(observer); }
    const file = (await createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 20_000 }).mutate(() => {})).file;
    invariants(file);
    const ids = new Set(file.entries.map((entry) => entry.id));
    assert.ok(reported.length >= 40, `the surviving workers reported their updates (${reported.length})`);
    assert.deepEqual(reported.filter((id) => !ids.has(id)), [], "every update reported as applied is in the queue");
  });
  it("succeeds a dead holder by taking the next generation, without removing its lock", async () => {
    store.read();
    const dead = holdQueueLock(tmp, store.repo, findDeadPid());
    assert.equal((await store.enqueue(owner(1))).entry.state, "active");
    assert.ok(existsSync(dead.path), "a reclaimed lock is only collected far behind the newest generation");
    assert.equal(lockFile(dead.gen + 1).after, "reclaimed");
  });
  it("reclaims a lock whose live holder exceeded the lease, and an unpublished lock of this process at once", async () => {
    store.read();
    // Its stamp is older than the lease by the monotonic clock (its file mtime is fresh: only the stamp counts).
    holdQueueLock(tmp, store.repo, process.ppid, "committed", { mono: logClock.mono() - STALE_LOCK_MS - 1000 });
    assert.equal((await store.enqueue(owner(1))).entry.state, "active");
    // A critical section never awaits, so between attempts an unpublished lock of this pid was abandoned.
    const own = holdQueueLock(tmp, store.repo, process.pid);
    const started = Date.now();
    assert.equal((await store.enqueue(owner(2))).position, 2);
    assert.ok(Date.now() - started < STALE_LOCK_MS / 2);
    assert.equal(lockFile(own.gen + 1).after, "reclaimed");
  });
  it("does not steal live locks and bounds its acquisition wait", async () => {
    store.read();
    const live = holdQueueLock(tmp, store.repo);
    const impatient = createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 35 });
    await assert.rejects(impatient.enqueue(owner(1)), /lock timed out/);
    assert.equal(lockFile(live.gen).token, `test-${live.gen}`);
    assert.equal(existsSync(join(store.path, lockName(live.gen + 1))), false);
    live.release();
    assert.equal((await impatient.enqueue(owner(1))).entry.state, "active");
    assert.equal(lockFile(live.gen + 1).after, "committed");
  });
  it("drops dead-pid entries and promotes the next waiter", async () => {
    const first = await store.enqueue(owner(1));
    const next = await store.enqueue(owner(2));
    const file = store.read(); file.entries[0]!.pid = findDeadPid();
    writeQueueSnapshot(tmp, store.repo, file);
    const result = await store.mutate(() => {});
    assert.equal(result.promoted?.id, next.entry.id);
    assert.ok(!result.file.entries.some((entry) => entry.id === first.entry.id));
  });
  it("idle expiry promotes the next entry with a fresh idle grace", async () => {
    const first = await store.enqueue(owner(1)); const next = await store.enqueue(owner(2));
    await store.setHoldExpiry(first.entry.id, null);
    clock += 600_001;
    assert.equal((await store.mutate(() => {})).file.entries[0]!.id, first.entry.id);
    await store.setHoldExpiry(first.entry.id, clock + 100);
    clock += 100;
    const result = await store.mutate(() => {});
    assert.equal(result.promoted?.id, next.entry.id);
    assert.equal(result.promoted?.holdExpiresAt, clock + 600_000);
    assert.equal(result.promoted?.grantedAt, new Date(clock).toISOString());
  });
  it("treats malformed files as empty and drops malformed entries individually", async () => {
    store.read(); writeQueueSnapshot(tmp, store.repo, "not JSON"); assert.equal(store.read().entries.length, 0);
    writeQueueSnapshot(tmp, store.repo, JSON.stringify({ v: 2, repo: store.repo, entries: [] })); assert.equal(store.read().entries.length, 0);
    const first = await store.enqueue(owner(1));
    writeQueueSnapshot(tmp, store.repo, JSON.stringify({ v: 1, repo: store.repo, entries: [null, {}, { ...first.entry, pid: -1 },
      { ...first.entry, id: "not uuid" }, { ...first.entry, holdExpiresAt: "soon" }, first.entry] }));
    assert.deepEqual(store.read().entries, [first.entry]);
    assert.equal((await store.enqueue(owner(2))).position, 2);
  });
  it("repairs duplicate owners, extra active entries and active placement", async () => {
    const a = (await store.enqueue(owner(1))).entry; const b = (await store.enqueue(owner(2))).entry;
    const c = (await store.enqueue(owner(3))).entry;
    const file: QueueFile = { v: 1, repo: store.repo, entries: [b, a, { ...c, state: "active" }, { ...b }] };
    normalizeQueue(file, clock, 600_000); invariants(file);
    assert.deepEqual(file.entries.map((entry) => entry.id), [a.id, b.id, c.id]);
  });
  it("rejects queue directory and snapshot symlinks", async () => {
    symlinkSync(tmp, join(tmp, "queue"));
    await assert.rejects(store.enqueue(owner(1)), /symbolic link/);
  });
  it("never follows a queue snapshot symlink", async () => {
    store.read(); const target = join(tmp, "target"); writeFileSync(target, "private");
    symlinkSync(target, join(store.path, snapshotName(holdQueueLock(tmp, store.repo).gen)));
    await assert.rejects(store.enqueue(owner(1)));
    assert.equal(readFileSync(target, "utf8"), "private");
  });
});

describe("queue lock generations", () => {
  it("a contender that saw a stale lock cannot remove the lock of the holder that succeeded it (v1 lost update)", async () => {
    store.read();
    holdQueueLock(tmp, store.repo, findDeadPid()); // L1: its holder died inside the critical section
    let d: HeldQueueLock | undefined;
    // C has judged L1 stale; right before C creates L2, B succeeds L1 and commits, and D takes L3.
    const restore = interceptOnce("linkSync", () => {
      holdQueueLock(tmp, store.repo, process.ppid, "reclaimed").release();
      d = holdQueueLock(tmp, store.repo);
    });
    const c = store.enqueue(owner(0xc));
    let settled = false; void c.finally(() => { settled = true; });
    try { await new Promise((resolve) => setTimeout(resolve, 80)); } finally { restore(); }
    assert.equal(settled, false, "C waits for D instead of taking D's turn");
    assert.equal(lockFile(d!.gen).token, `test-${d!.gen}`, "D's lock is intact");
    // D finishes its critical section from its own read of the queue.
    const dEntry = entryOf(0xd);
    writeFileSync(join(store.path, snapshotName(d!.gen)), JSON.stringify({ v: 1, repo: store.repo, entries: [dEntry] }), { mode: 0o600 });
    const result = await c;
    assert.deepEqual(result.file.entries.map((entry) => entry.endpointId), [dEntry.endpointId, owner(0xc).endpointId]);
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId), [dEntry.endpointId, owner(0xc).endpointId]);
  });
  it("two reclaimers of one stale generation take consecutive generations and lose no update", async () => {
    store.read();
    const dead = holdQueueLock(tmp, store.repo, findDeadPid());
    const other = createQueueStore({ busDir: tmp, repo: store.repo, idleMs: 600_000, now: () => clock });
    await Promise.all([store.enqueue(owner(1)), other.enqueue(owner(2))]);
    assert.deepEqual([lockFile(dead.gen + 1).after, lockFile(dead.gen + 2).after], ["reclaimed", "committed"]);
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId).sort(), [owner(1).endpointId, owner(2).endpointId]);
  });
  it("a holder reclaimed while paused in its critical section reports failure and never overwrites its successor", async () => {
    await store.enqueue(owner(1));
    let successor: HeldQueueLock | undefined;
    const successorFile: QueueFile = { v: 1, repo: store.repo, entries: [{ ...entryOf(9), state: "active", grantedAt: new Date().toISOString(), holdExpiresAt: null }] };
    await assert.rejects(store.mutate((file) => {
      file.entries.push(entryOf(2));
      // Paused past the lease: a successor reclaimed this generation and committed its own snapshot.
      successor = holdQueueLock(tmp, store.repo, process.ppid, "reclaimed");
      writeFileSync(join(store.path, snapshotName(successor.gen)), JSON.stringify(successorFile), { mode: 0o600 });
    }), QueueFencedError);
    assert.equal(existsSync(join(store.path, snapshotName(successor!.gen - 1))), false, "the late update is not published");
    assert.equal(lockFile(successor!.gen).token, `test-${successor!.gen}`);
    assert.deepEqual(newestSnapshot(), successorFile);
  });
  it("a successor that reclaimed the generation before its snapshot appeared makes the holder report failure", async () => {
    await store.enqueue(owner(1));
    let successor: HeldQueueLock | undefined;
    const restore = interceptOnce("renameSync", () => { successor = holdQueueLock(tmp, store.repo, process.ppid, "reclaimed"); });
    try { await assert.rejects(store.enqueue(owner(2)), QueueFencedError); } finally { restore(); }
    assert.equal(lockFile(successor!.gen).token, `test-${successor!.gen}`, "the successor's lock is intact");
  });
  it("releases a failed mutation unchanged so the next mutation does not wait for the lease", async () => {
    await store.enqueue(owner(1));
    await assert.rejects(store.mutate(() => { throw new Error("boom"); }), /boom/);
    const impatient = createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 200 });
    assert.deepEqual((await impatient.enqueue(owner(2))).file.entries.map((entry) => entry.endpointId), [owner(1).endpointId, owner(2).endpointId]);
  });
  it("collects only generations far behind an anchor whose lock is old by the monotonic clock, bottom-up", async () => {
    store.read();
    const stray = join(store.path, `T${"0".repeat(15)}.${crypto.randomUUID()}.s.tmp`);
    writeFileSync(stray, "partial");
    for (let i = 0; i < KEEP_GENERATIONS + 8; i++) await store.mutate(() => {});
    assert.equal(existsSync(join(store.path, lockName(1))), true, "recent generations are kept however many there are");
    elapse(COLLECT_AGE_MS + 1000);
    // A paused process creates the collected name L4 again while the collector enumerates, and the scan misses it.
    collectGenerations(store.path, KEEP_GENERATIONS + 4);
    assert.equal(newestLock() - KEEP_GENERATIONS - 8, 0);
    assert.equal(existsSync(stray), false, "a temporary file of a generation far behind is collected");
    assert.deepEqual([existsSync(join(store.path, lockName(4))), existsSync(join(store.path, lockName(5)))], [false, true]);
    writeFileSync(join(store.path, lockName(4)), JSON.stringify({ v: 2, pid: process.ppid, token: "late", after: "committed" }), { mode: 0o600 });
    const restore = interceptOnce("readdirSync", () => fs.readdirSync(store.path).filter((name) => name !== lockName(4)), (path) => path === store.path);
    try { collectGenerations(store.path, KEEP_GENERATIONS + 8); } finally { restore(); }
    assert.equal(existsSync(join(store.path, lockName(5))), true, "nothing above a remaining older lock is collected");
    collectGenerations(store.path, KEEP_GENERATIONS + 8);
    assert.equal(Math.min(...readdirSync(store.path).filter((name) => name.startsWith("L")).map((name) => Number(name.slice(1)))), 9);
    assert.equal(existsSync(join(store.path, snapshotName(KEEP_GENERATIONS + 8))), true);
  });
  it("a holder paused before publishing while others advance far and collect reports failure, not success", async () => {
    await store.enqueue(owner(1));
    const base = store.read();
    let newest = 0;
    await assert.rejects(store.mutate((file) => {
      file.entries.push(entryOf(2));
      newest = othersAdvance(newestLock(), KEEP_GENERATIONS * 2 + 8, base);
    }), QueueFencedError);
    assert.equal(existsSync(join(store.path, lockName(newest - KEEP_GENERATIONS * 2 - 7))), false, "its successor was collected");
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId), [owner(1).endpointId]);
  });
  it("a holder paused while publishing (before its rename) while others advance far and collect reports failure", async () => {
    await store.enqueue(owner(1));
    const base = store.read();
    const restore = interceptOnce("renameSync", () => { othersAdvance(newestLock(), KEEP_GENERATIONS * 2 + 8, base); });
    try { await assert.rejects(store.enqueue(owner(2)), QueueFencedError); } finally { restore(); }
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId), [owner(1).endpointId]);
  });
  it("a contender paused before creating its lock while others advance far and collect gives the re-created name up", async () => {
    await store.enqueue(owner(1));
    const base = store.read();
    const m = newestLock();
    let newest = 0;
    // Others advance far and collect while it is paused (time passes for everyone, so its window has expired).
    // Paused after judging L<m> ended, before writing its lock: its temporary file does not exist yet.
    const restore = interceptOnce("openSync", () => { newest = othersAdvance(m, KEEP_GENERATIONS * 2 + 8, base, "committed"); },
      (path) => path.endsWith(".l.tmp"));
    try { await store.enqueue(owner(0xc)); } finally { restore(); }
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId), [owner(1).endpointId, owner(0xc).endpointId]);
    assert.deepEqual([lockFile(newest + 1).pid, lockFile(newest + 1).after], [process.pid, "committed"]);
  });
  it("a wall clock set back during that pause does not shorten the contender's window", async () => {
    await store.enqueue(owner(1));
    const base = store.read();
    const m = newestLock();
    let newest = 0;
    const restore = interceptOnce("openSync", () => {
      newest = othersAdvance(m, KEEP_GENERATIONS * 2 + 8, base, "committed");
      setWallClock(-3_600_000); // by the wall clock, less time has passed than before the pause
    }, (path) => path.endsWith(".l.tmp"));
    try { await store.enqueue(owner(0xc)); } finally { restore(); }
    assert.deepEqual(store.read().entries.map((entry) => entry.endpointId), [owner(1).endpointId, owner(0xc).endpointId]);
    assert.deepEqual([lockFile(newest + 1).pid, lockFile(newest + 1).after], [process.pid, "committed"],
      "the re-created name was given up and the turn taken after the newest generation");
  });
  it("a wall clock set forward neither expires a live holder's lease nor ages generations for collection", async () => {
    store.read();
    for (let i = 0; i < KEEP_GENERATIONS + 8; i++) await store.mutate(() => {});
    const live = holdQueueLock(tmp, store.repo);
    setWallClock(3_600_000); // files written before now look an hour old by the wall clock
    for (const name of readdirSync(store.path)) age(join(store.path, name), 3_600_000);
    collectGenerations(store.path, live.gen - 1);
    assert.equal(existsSync(join(store.path, lockName(1))), true, "nothing is collected: no lock is old by the monotonic clock");
    const impatient = createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 60 });
    await assert.rejects(impatient.enqueue(owner(1)), /lock timed out/);
    assert.equal(existsSync(join(store.path, lockName(live.gen + 1))), false, "the live holder was not reclaimed");
    live.release();
    assert.equal((await impatient.enqueue(owner(1))).entry.state, "active");
  });
  it("a lock of an earlier boot is over at once and never serves as an anchor; an unstamped lock uses its mtime", async () => {
    store.read();
    logClock.boot = "boot-now";
    for (let i = 0; i < KEEP_GENERATIONS + 4; i++) holdQueueLock(tmp, store.repo, process.ppid, "committed", { boot: "boot-before", mono: 1 }).release();
    const before = holdQueueLock(tmp, store.repo, process.ppid, "committed", { boot: "boot-before", mono: logClock.mono() });
    collectGenerations(store.path, before.gen);
    assert.equal(existsSync(join(store.path, lockName(1))), true, "stamps of another boot are not compared with this boot's clock");
    assert.equal((await store.enqueue(owner(1))).entry.state, "active", "its live-looking pid is from the earlier boot");
    assert.equal(lockFile(before.gen + 1).after, "reclaimed");
    const unstamped = holdQueueLock(tmp, store.repo, process.ppid, "committed", null);
    const impatient = createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 60 });
    await assert.rejects(impatient.enqueue(owner(2)), /lock timed out/);
    age(unstamped.path, STALE_LOCK_MS + 1000);
    assert.equal((await impatient.enqueue(owner(2))).position, 2);
  });
  it("a contender that finds a newer generation after creating its lock voids it and keeps the name (no holes)", async () => {
    await store.enqueue(owner(1));
    const m = newestLock();
    let taker: HeldQueueLock | undefined;
    // Paused after creating L<m+1>: another process judged it stale and took L<m+2>.
    const restore = interceptOnce("lstatSync", () => { taker = holdQueueLock(tmp, store.repo, process.ppid, "reclaimed"); },
      (path) => path.endsWith(lockName(m + 2)));
    const c = store.enqueue(owner(2));
    try { await new Promise((resolve) => setTimeout(resolve, 60)); } finally { restore(); }
    assert.deepEqual([existsSync(join(store.path, lockName(m + 1))), existsSync(join(store.path, voidName(m + 1)))], [true, true]);
    taker!.release();
    assert.deepEqual((await c).file.entries.map((entry) => entry.endpointId), [owner(1).endpointId, owner(2).endpointId]);
  });
  it("a scan that misses a generation created during it only makes the attempt stale", async () => {
    await store.enqueue(owner(1));
    let other: HeldQueueLock | undefined;
    const restore = interceptOnce("readdirSync", () => {
      const listing = fs.readdirSync(store.path); // the enumeration ends before the other process creates its lock
      other = holdQueueLock(tmp, store.repo);
      return listing;
    }, (path) => path === store.path);
    const c = store.enqueue(owner(2));
    try { await new Promise((resolve) => setTimeout(resolve, 60)); } finally { restore(); }
    assert.equal(existsSync(join(store.path, lockName(other!.gen + 1))), false, "it waits for the generation it did not see");
    const file = store.read();
    other!.publish({ ...file, entries: [...file.entries, entryOf(3)] });
    assert.deepEqual((await c).file.entries.map((entry) => entry.endpointId), [owner(1).endpointId, owner(3).endpointId, owner(2).endpointId]);
  });
  it("refuses while an older session holds the v1 turn or lock, and never changes v1 files", async () => {
    store.read();
    const legacyQueue = join(tmp, "queue", `${store.key}.json`);
    const legacyLock = join(tmp, "queue", `${store.key}.lock`);
    const holder = { ...entryOf(7, process.ppid), state: "active", grantedAt: new Date(clock).toISOString(), holdExpiresAt: null };
    const v1 = JSON.stringify({ v: 1, repo: store.repo, entries: [holder] });
    writeFileSync(legacyQueue, v1, { mode: 0o600 });
    await assert.rejects(store.enqueue(owner(1)), (err: unknown) => err instanceof LegacyQueueError && err.pids.includes(process.ppid));
    assert.equal(readFileSync(legacyQueue, "utf8"), v1);
    // Dead, expired and own-process v1 holders, and v1 waiters, are not honored.
    writeFileSync(legacyQueue, JSON.stringify({ v: 1, repo: store.repo, entries: [{ ...holder, pid: findDeadPid() },
      { ...entryOf(8, process.ppid), holdExpiresAt: null }] }), { mode: 0o600 });
    assert.equal((await store.enqueue(owner(1))).entry.state, "active");
    writeFileSync(legacyQueue, JSON.stringify({ v: 1, repo: store.repo, entries: [{ ...holder, holdExpiresAt: clock - 1 }] }), { mode: 0o600 });
    await store.mutate(() => {});
    // A fresh v1 lock of a live foreign process is an older session in its critical section.
    writeFileSync(legacyLock, JSON.stringify({ pid: process.ppid, token: "v1" }), { mode: 0o600 });
    await assert.rejects(store.mutate(() => {}), LegacyQueueError);
    const old = new Date(Date.now() - STALE_LOCK_MS - 1000); utimesSync(legacyLock, old, old);
    await store.mutate(() => {});
    assert.equal(JSON.parse(readFileSync(legacyLock, "utf8")).token, "v1");
    assert.equal(store.read().entries.length, 1);
  });
});

describe("repository queue, continued", () => {
  it("voids a generation whose snapshot cannot be read, and publishes nothing", async () => {
    await store.enqueue(owner(1));
    const before = readdirSync(store.path).length;
    const held = holdQueueLock(tmp, store.repo);
    symlinkSync(join(tmp, "missing"), join(store.path, snapshotName(held.gen)));
    await assert.rejects(store.mutate(() => {}));
    assert.equal(readdirSync(store.path).length, before + 4, "the symlinked snapshot, its lock, and the new lock with its void marker");
    assert.equal(existsSync(join(store.path, voidName(held.gen + 1))), true);
  });
});

describe("queue liveness and nudges", () => {
  const socket = (entry: QueueEntry) => join(tmp, socketFileName(entry.endpointId, entry.pid));
  it("reports a missing endpoint and a refused socket as dead", async () => {
    const entry = (await store.enqueue(owner(1))).entry;
    assert.equal(await probeHolder(tmp, entry), "dead");
    await makeStaleSocket(socket(entry));
    assert.equal(await probeHolder(tmp, entry), "dead");
  });
  it("keeps timeout, rejection and hello identity mismatches unknown", async () => {
    const entry = (await store.enqueue(owner(1))).entry;
    servers.push(await listenSilent(socket(entry)));
    assert.equal(await probeHolder(tmp, entry, { timeoutMs: 20 }), "unknown");
    await servers.pop()!.close();
    servers.push(await listenHello(socket(entry), peerInfo("ffffffff", process.pid)));
    assert.equal(await probeHolder(tmp, entry), "unknown");
    await servers.pop()!.close();
    servers.push(await listenWith(socket(entry), (sock) => sock.on("data", () => sock.end('{"v":1,"ok":false,"status":"rejected","reason":"no"}\n'))));
    assert.equal(await probeHolder(tmp, entry), "unknown");
  });
  it("recognizes a matching live endpoint", async () => {
    const entry = (await store.enqueue(owner(1))).entry;
    servers.push(await listenHello(socket(entry), peerInfo(entry.endpointId, entry.pid)));
    assert.equal(await probeHolder(tmp, entry), "alive");
  });
  it("skips stale promoted owners and handles local promotion without a socket", async () => {
    const first = (await store.enqueue(owner(1))).entry; const next = (await store.enqueue(owner(2))).entry;
    let local: QueueEntry | undefined;
    const final = await nudgePromoted(store, first, { ownEndpointId: next.endpointId, onOwn: (entry) => { local = entry; } });
    assert.equal(final?.id, next.id); assert.equal(local?.id, next.id);
    assert.equal(store.read().entries.length, 1);
  });
  it("nudges a promoted owner and leaves rejected nudges alone", async () => {
    const entry = (await store.enqueue(owner(1))).entry;
    let nudged: string | undefined;
    const endpoint = createEndpoint({ busDir: tmp, id: entry.endpointId, sessionId: entry.sessionId,
      getPeerInfo: () => ({ cwd: "/repo", busy: false, autoWake: false }),
      onNote: () => ({ v: 1, ok: true, status: "delivered", wake: "suppressed" }),
      onQueueNudge: (key) => { nudged = key; } });
    endpoints.push(endpoint); await endpoint.start();
    await nudgePromoted(store, entry);
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(nudged, store.key);
    await endpoint.stop();
    servers.push(await listenWith(socket(entry), (sock) => sock.on("data", () => sock.end('{"v":1,"ok":false,"status":"rejected","reason":"unsupported"}\n'))));
    await nudgePromoted(store, entry); assert.equal(store.read().entries[0]!.id, entry.id);
  });
  it("bounds stale-owner nudge cleanup", async () => {
    const first = (await store.enqueue(owner(1))).entry;
    await store.enqueue(owner(2)); await store.enqueue(owner(3));
    await nudgePromoted(store, first, { maxAttempts: 1 });
    assert.equal(store.read().entries.length, 2);
  });
});
