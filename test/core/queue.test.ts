import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, lstatSync, readFileSync, symlinkSync, utimesSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createQueueStore, normalizeQueue, nudgePromoted, probeHolder, queueKey, queueTitle, type QueueEntry, type QueueFile, type QueueStore } from "../../src/core/queue.ts";
import { createEndpoint, type Endpoint } from "../../src/core/endpoint.ts";
import { socketFileName } from "../../src/core/registry.ts";
import { findDeadPid, listenHello, listenSilent, listenWith, makeStaleSocket, makeTempDir, peerInfo, removeTempDir, type TestServer } from "./helpers.ts";

let tmp: string;
let store: QueueStore;
let clock: number;
let servers: TestServer[];
let endpoints: Endpoint[];
const owner = (n: number) => ({ endpointId: n.toString(16).padStart(8, "0"), pid: process.pid, sessionId: `session-${n}`, title: `task ${n}` });
beforeEach(() => {
  tmp = makeTempDir("sb-q"); clock = Date.now(); servers = []; endpoints = [];
  store = createQueueStore({ busDir: tmp, repo: "/repo/worktree", idleMs: 600_000, now: () => clock });
});
afterEach(async () => {
  await Promise.all(endpoints.map((endpoint) => endpoint.stop()));
  await Promise.all(servers.map((server) => server.close()));
  removeTempDir(tmp);
});
const lockPath = () => join(tmp, "queue", `${store.key}.lock`);
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
    assert.equal(lstatSync(store.path).mode & 0o777, 0o600);
    assert.equal(lstatSync(join(tmp, "queue")).mode & 0o777, 0o700);
    assert.equal(existsSync(lockPath()), false);
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
  it("reclaims locks owned by a dead pid", async () => {
    store.read();
    writeFileSync(lockPath(), JSON.stringify({ pid: findDeadPid(), token: "dead" }), { mode: 0o600 });
    assert.equal((await store.enqueue(owner(1))).entry.state, "active");
    assert.equal(existsSync(lockPath()), false);
  });
  it("reclaims locks with an old mtime even when their pid is alive", async () => {
    store.read();
    writeFileSync(lockPath(), JSON.stringify({ pid: process.pid, token: "old" }), { mode: 0o600 });
    const old = new Date(Date.now() - 11_000); utimesSync(lockPath(), old, old);
    assert.equal((await store.enqueue(owner(1))).entry.state, "active");
  });
  it("does not steal live locks and bounds its acquisition wait", async () => {
    store.read();
    writeFileSync(lockPath(), JSON.stringify({ pid: process.pid, token: "live" }), { mode: 0o600 });
    const impatient = createQueueStore({ busDir: tmp, repo: store.repo, lockTimeoutMs: 35 });
    await assert.rejects(impatient.enqueue(owner(1)), /lock timed out/);
    assert.equal(JSON.parse(readFileSync(lockPath(), "utf8")).token, "live");
  });
  it("drops dead-pid entries and promotes the next waiter", async () => {
    const first = await store.enqueue(owner(1));
    const next = await store.enqueue(owner(2));
    const file = store.read(); file.entries[0]!.pid = findDeadPid();
    writeFileSync(store.path, JSON.stringify(file));
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
    store.read(); writeFileSync(store.path, "not JSON"); assert.equal(store.read().entries.length, 0);
    writeFileSync(store.path, JSON.stringify({ v: 2, repo: store.repo, entries: [] })); assert.equal(store.read().entries.length, 0);
    const first = await store.enqueue(owner(1));
    writeFileSync(store.path, JSON.stringify({ v: 1, repo: store.repo, entries: [null, {}, { ...first.entry, pid: -1 },
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
    store.read(); const target = join(tmp, "target"); writeFileSync(target, "private"); symlinkSync(target, store.path);
    await assert.rejects(store.enqueue(owner(1)));
    assert.equal(readFileSync(target, "utf8"), "private");
  });
  it("does not unlink a replacement lock carrying another token", async () => {
    await store.mutate(() => {
      writeFileSync(lockPath(), JSON.stringify({ pid: process.pid, token: "replacement" }));
    });
    assert.equal(JSON.parse(readFileSync(lockPath(), "utf8")).token, "replacement");
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
