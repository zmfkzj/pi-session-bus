import assert from "node:assert/strict";
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  statSync,
  symlinkSync,
  writeFileSync,
} from "node:fs";
import { createConnection } from "node:net";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  BusClientError,
  createEndpoint,
  helloProbe,
  isStaleError,
  listPeers,
  sendNote,
  type Endpoint,
  type EndpointOptions,
  type LivePeerInfo,
} from "../../src/core/endpoint.ts";
import {
  createNote,
  encodeFrame,
  type NoteRequest,
  type NoteResponse,
} from "../../src/core/protocol.ts";
import { writeEntry } from "../../src/core/registry.ts";
import { listenSilent, makeStaleSocket, makeTempDir, rawExchange, removeTempDir } from "./helpers.ts";

const mode = (path: string): number => statSync(path).mode & 0o777;
const DELIVERED: NoteResponse = { v: 1, ok: true, status: "delivered", wake: "started" };

let tmp: string;
let busDir: string;
let endpoints: Endpoint[];

beforeEach(() => {
  tmp = makeTempDir("sb-ep");
  busDir = join(tmp, "bus");
  endpoints = [];
});

afterEach(async () => {
  await Promise.all(endpoints.map((e) => e.stop()));
  removeTempDir(tmp);
});

interface Harness {
  endpoint: Endpoint;
  notes: NoteRequest[];
  live: LivePeerInfo;
}

function makeEndpoint(overrides: Partial<EndpointOptions> = {}, sessionId = "session-A"): Harness {
  const notes: NoteRequest[] = [];
  const live: LivePeerInfo = { name: "alpha", cwd: "/work/a", busy: false, autoWake: true };
  const endpoint = createEndpoint({
    busDir,
    sessionId,
    getPeerInfo: () => live,
    onNote: (note) => {
      notes.push(note);
      return DELIVERED;
    },
    ...overrides,
  });
  endpoints.push(endpoint);
  return { endpoint, notes, live };
}

function noteTo(to: string, overrides: Partial<NoteRequest> = {}): NoteRequest {
  return createNote({
    from: { id: "feedbeef", sessionId: "session-sender", name: "sender", cwd: "/work/s", replyable: true },
    to,
    content: "hello",
    hops: 1,
    ...overrides,
  });
}

async function rejection(promise: Promise<unknown>): Promise<BusClientError> {
  try {
    await promise;
  } catch (err) {
    assert.ok(err instanceof BusClientError, `expected BusClientError, got ${String(err)}`);
    return err;
  }
  throw new assert.AssertionError({ message: "expected the promise to reject" });
}

describe("endpoint start", () => {
  it("creates a 0700 bus dir, a 0600 socket and a 0600 registry entry", async () => {
    const { endpoint } = makeEndpoint({ pid: 31337 });
    const entry = await endpoint.start();
    assert.equal(endpoint.running, true);
    assert.match(entry.id, /^[0-9a-f]{8}$/);
    assert.equal(entry.sessionId, "session-A");
    assert.equal(entry.pid, 31337);
    assert.equal(entry.socket, join(busDir, `${entry.id}.sock`));
    assert.ok(!Number.isNaN(Date.parse(entry.startedAt)));

    assert.equal(mode(busDir), 0o700);
    assert.equal(mode(entry.socket), 0o600);
    assert.ok(statSync(entry.socket).isSocket());
    const file = join(busDir, `${entry.id}.json`);
    assert.equal(mode(file), 0o600);
    assert.deepEqual(JSON.parse(readFileSync(file, "utf8")), entry);
    assert.deepEqual(readdirSync(busDir).sort(), [`${entry.id}.json`, `${entry.id}.sock`]);
  });

  it("tightens a permissive bus dir we own", async () => {
    mkdirSync(busDir, { mode: 0o755 });
    chmodSync(busDir, 0o755);
    await makeEndpoint().endpoint.start();
    assert.equal(mode(busDir), 0o700);
  });

  it("refuses a symlinked bus dir", async () => {
    const real = join(tmp, "real");
    mkdirSync(real, { mode: 0o700 });
    symlinkSync(real, busDir);
    const { endpoint } = makeEndpoint();
    await assert.rejects(endpoint.start(), /symbolic link/);
    assert.equal(endpoint.running, false);
    assert.deepEqual(readdirSync(real), []);
  });

  it("derives a stable id from sessionId + pid that differs between processes", async () => {
    const a = makeEndpoint({ pid: 1001 }, "same-session");
    const b = makeEndpoint({ pid: 1002 }, "same-session");
    const [ea, eb] = [await a.endpoint.start(), await b.endpoint.start()];
    assert.notEqual(ea.id, eb.id);
    await a.endpoint.stop();
    const again = makeEndpoint({ pid: 1001 }, "same-session");
    assert.equal((await again.endpoint.start()).id, ea.id);
  });

  it("cannot be started twice or after stop", async () => {
    const { endpoint } = makeEndpoint();
    await endpoint.start();
    await assert.rejects(endpoint.start(), /already running/);
    await endpoint.stop();
    await assert.rejects(endpoint.start(), /already stopped/);
  });
});

describe("hello", () => {
  it("answers with live peer data", async () => {
    const { endpoint, live } = makeEndpoint({ pid: 777 });
    const entry = await endpoint.start();
    assert.deepEqual(await helloProbe(entry.socket), {
      id: entry.id,
      sessionId: "session-A",
      name: "alpha",
      cwd: "/work/a",
      pid: 777,
      busy: false,
      autoWake: true,
      receiving: true,
    });
    live.busy = true;
    live.autoWake = false;
    delete live.name;
    const second = await helloProbe(entry.socket);
    assert.equal(second.busy, true);
    assert.equal(second.autoWake, false);
    assert.equal("name" in second, false);
  });

  it("truncates oversized live data so the probe still validates", async () => {
    const { endpoint } = makeEndpoint({
      getPeerInfo: () => ({ name: "n".repeat(1000), cwd: "/".repeat(10_000), busy: false, autoWake: true }),
    });
    const entry = await endpoint.start();
    const peer = await helloProbe(entry.socket);
    assert.equal(peer.name?.length, 256);
    assert.equal(peer.cwd.length, 4096);
  });

  it("answers invalid versions, types and JSON with a rejection and closes", async () => {
    const { endpoint } = makeEndpoint();
    const { socket } = await endpoint.start();
    for (const [payload, pattern] of [
      ['{"v":2,"type":"hello"}\n', /version/],
      ['{"v":1,"type":"redirect"}\n', /type/],
      ["not json\n", /invalid JSON/],
      ['{"v":1,"type":"note"}\n', /note\./],
    ] as const) {
      const res = await rawExchange(socket, payload);
      assert.equal(res.timedOut, false, payload);
      const parsed = JSON.parse(res.received.trim());
      assert.equal(parsed.ok, false);
      assert.equal(parsed.status, "rejected");
      assert.match(parsed.reason, pattern);
    }
  });

  it("takes one request per connection: extra frames are ignored", async () => {
    const { endpoint } = makeEndpoint();
    const { socket } = await endpoint.start();
    const hello = encodeFrame({ v: 1, type: "hello" }).toString("utf8");
    const res = await rawExchange(socket, hello + hello);
    assert.equal(res.received.trim().split("\n").length, 1);
  });

  it("also works with a client that half-closes after sending", async () => {
    const { endpoint } = makeEndpoint();
    const { socket } = await endpoint.start();
    const text = await new Promise<string>((resolve, reject) => {
      const chunks: Buffer[] = [];
      const sock = createConnection(socket);
      sock.on("data", (c) => chunks.push(c));
      sock.on("close", () => resolve(Buffer.concat(chunks).toString("utf8")));
      sock.on("error", reject);
      sock.end(encodeFrame({ v: 1, type: "hello" }));
    });
    assert.equal(JSON.parse(text).ok, true);
  });
});

describe("note delivery", () => {
  it("delivers a note to onNote and returns its answer", async () => {
    const { endpoint, notes } = makeEndpoint();
    const entry = await endpoint.start();
    const note = noteTo(entry.id, { content: "plan: use \u2028 and \n newlines", replyTo: "m0" });
    const res = await sendNote(entry.socket, note);
    assert.deepEqual(res, DELIVERED);
    assert.equal(notes.length, 1);
    assert.deepEqual(notes[0], note);
  });

  it("passes suppressed answers (with reason) through", async () => {
    const { endpoint } = makeEndpoint({
      onNote: () => ({ v: 1, ok: true, status: "delivered", wake: "suppressed", reason: "hop_limit" }),
    });
    const entry = await endpoint.start();
    const res = await sendNote(entry.socket, noteTo(entry.id, { hops: 9 }));
    assert.equal(res.wake, "suppressed");
    assert.equal(res.reason, "hop_limit");
  });

  it("supports async onNote", async () => {
    const { endpoint } = makeEndpoint({
      onNote: async () => {
        await new Promise((r) => setTimeout(r, 30));
        return { v: 1, ok: true, status: "delivered", wake: "queued" };
      },
    });
    const entry = await endpoint.start();
    assert.equal((await sendNote(entry.socket, noteTo(entry.id))).wake, "queued");
  });

  it("answers a repeated message id with 'duplicate' and does not deliver again", async () => {
    const { endpoint, notes } = makeEndpoint();
    const entry = await endpoint.start();
    const note = noteTo(entry.id);
    assert.equal((await sendNote(entry.socket, note)).status, "delivered");
    const second = await sendNote(entry.socket, note);
    assert.equal(second.status, "duplicate");
    assert.equal(second.wake, "started"); // the original wake status
    assert.equal(notes.length, 1);
    await sendNote(entry.socket, noteTo(entry.id)); // different id => delivered
    assert.equal(notes.length, 2);
  });

  it("deduplicates concurrent sends of the same id", async () => {
    let calls = 0;
    const { endpoint } = makeEndpoint({
      onNote: async () => {
        calls++;
        await new Promise((r) => setTimeout(r, 50));
        return DELIVERED;
      },
    });
    const entry = await endpoint.start();
    const note = noteTo(entry.id);
    const results = await Promise.all([sendNote(entry.socket, note), sendNote(entry.socket, note)]);
    assert.equal(calls, 1);
    assert.deepEqual(results.map((r) => r.status).sort(), ["delivered", "duplicate"]);
  });

  it("bounds the dedup cache: an evicted id is delivered again", async () => {
    const { endpoint, notes } = makeEndpoint({ limits: { dedupSize: 2 } });
    const entry = await endpoint.start();
    const [n1, n2, n3] = [noteTo(entry.id), noteTo(entry.id), noteTo(entry.id)] as const;
    for (const n of [n1, n2, n3]) await sendNote(entry.socket, n);
    assert.equal((await sendNote(entry.socket, n3)).status, "duplicate");
    assert.equal((await sendNote(entry.socket, n1)).status, "delivered"); // evicted
    assert.equal(notes.length, 4);
  });

  it("does not cache rejections or failures: a retry is delivered", async () => {
    let attempt = 0;
    const { endpoint, notes } = makeEndpoint({
      onNote: (note) => {
        attempt++;
        if (attempt === 1) return { v: 1, ok: false, status: "rejected", reason: "busy right now" };
        if (attempt === 2) throw new Error("boom");
        notes.push(note);
        return DELIVERED;
      },
    });
    const entry = await endpoint.start();
    const note = noteTo(entry.id);
    const first = await rejection(sendNote(entry.socket, note));
    assert.equal(first.code, "rejected");
    assert.equal(first.reason, "busy right now");
    const second = await rejection(sendNote(entry.socket, note));
    assert.equal(second.reason, "internal error");
    assert.equal((await sendNote(entry.socket, note)).status, "delivered");
    assert.equal(notes.length, 1);
  });

  it("rejects a note whose `to` is not this endpoint and never calls onNote", async () => {
    const { endpoint, notes } = makeEndpoint();
    const entry = await endpoint.start();
    const err = await rejection(sendNote(entry.socket, noteTo("00000000")));
    assert.equal(err.code, "rejected");
    assert.match(err.reason ?? "", /wrong recipient/);
    assert.equal(notes.length, 0);
  });

  it("rejects self-addressed notes", async () => {
    const { endpoint, notes } = makeEndpoint();
    const entry = await endpoint.start();
    const note = noteTo(entry.id, {
      from: { id: entry.id, sessionId: "session-A", cwd: "/work/a", replyable: true },
    });
    const err = await rejection(sendNote(entry.socket, note));
    assert.match(err.reason ?? "", /self-addressed/);
    assert.equal(notes.length, 0);
  });

  it("enforces the content cap on the server (client limit bypassed)", async () => {
    const { endpoint, notes } = makeEndpoint({ limits: { maxContentBytes: 100 } });
    const entry = await endpoint.start();
    const err = await rejection(sendNote(entry.socket, noteTo(entry.id, { content: "x".repeat(101) })));
    assert.match(err.reason ?? "", /exceeds 100 bytes/);
    assert.equal(notes.length, 0);
    assert.equal((await sendNote(entry.socket, noteTo(entry.id, { content: "x".repeat(100) }))).status, "delivered");
  });

  it("the client reports too_large before connecting (content and frame caps)", async () => {
    const nowhere = join(tmp, "nowhere.sock");
    const bigContent = await rejection(sendNote(nowhere, noteTo("00000000", { content: "x".repeat(32769) })));
    assert.equal(bigContent.code, "too_large");
    // 32768 control characters fit the content cap but expand to 6 bytes each as JSON
    const bigFrame = await rejection(sendNote(nowhere, noteTo("00000000", { content: "\u0001".repeat(32768) })));
    assert.equal(bigFrame.code, "too_large");
  });

  it("round-trips a maximum-size note", async () => {
    const { endpoint, notes } = makeEndpoint();
    const entry = await endpoint.start();
    const content = "é".repeat(16384); // exactly 32768 bytes
    await sendNote(entry.socket, noteTo(entry.id, { content }));
    assert.equal(notes[0]!.content, content);
  });
});

describe("connection limits", () => {
  it("destroys a connection that exceeds the frame cap (with or without LF)", async () => {
    const { endpoint, notes } = makeEndpoint({ limits: { maxFrameBytes: 512 } });
    const { socket } = await endpoint.start();
    const noLf = await rawExchange(socket, "a".repeat(2000));
    assert.equal(noLf.closed, true);
    assert.equal(noLf.received, "");
    const withLf = await rawExchange(socket, `${"a".repeat(2000)}\n`);
    assert.equal(withLf.closed, true);
    assert.equal(withLf.received, "");
    const bigNote = await rawExchange(socket, encodeFrame(noteTo("whatever", { content: "x".repeat(1000) })));
    assert.equal(bigNote.closed, true);
    assert.equal(bigNote.received, "");
    assert.equal(notes.length, 0);
    assert.equal((await helloProbe(socket)).sessionId, "session-A"); // still serving
  });

  it("destroys idle connections after idleTimeoutMs", async () => {
    const { endpoint } = makeEndpoint({ limits: { idleTimeoutMs: 80 } });
    const { socket } = await endpoint.start();
    const started = Date.now();
    const silent = await rawExchange(socket, undefined, 2000);
    assert.equal(silent.closed, true);
    assert.ok(Date.now() - started < 1500);
    const partial = await rawExchange(socket, '{"v":1,"type":"hel', 2000);
    assert.equal(partial.closed, true);
    assert.equal(partial.received, "");
    assert.equal((await helloProbe(socket)).receiving, true);
  });

  it("closes the connection after the response even when the client lingers", async () => {
    const { endpoint } = makeEndpoint({ limits: { idleTimeoutMs: 100 } });
    const { socket } = await endpoint.start();
    const res = await rawExchange(socket, encodeFrame({ v: 1, type: "hello" }), 2000);
    assert.equal(res.closed, true);
    assert.equal(JSON.parse(res.received).ok, true);
  });
});

describe("stale pruning (listPeers)", () => {
  it("lists live peers, excludes self, and reports live data", async () => {
    const a = makeEndpoint({}, "session-A");
    const b = makeEndpoint({ getPeerInfo: () => ({ name: "beta", cwd: "/work/b", busy: true, autoWake: false }) }, "session-B");
    const ea = await a.endpoint.start();
    const eb = await b.endpoint.start();
    const fromA = await listPeers(busDir, ea.id);
    assert.deepEqual(
      fromA.map((p) => [p.id, p.name, p.busy, p.autoWake, p.socket]),
      [[eb.id, "beta", true, false, eb.socket]],
    );
    const fromNobody = await listPeers(busDir, undefined);
    assert.deepEqual(fromNobody.map((p) => p.id).sort(), [ea.id, eb.id].sort());
  });

  it("prunes entries whose socket is gone (ENOENT) or refuses connections (ECONNREFUSED)", async () => {
    const live = makeEndpoint({}, "session-live");
    const el = await live.endpoint.start();

    // ENOENT: registry entry without a socket file
    writeEntry(busDir, { v: 1, id: "dead0001", sessionId: "s1", pid: 1, socket: join(busDir, "dead0001.sock"), startedAt: "2026-10-02T10:00:00.000Z" });
    // ECONNREFUSED: socket file left behind by a killed process
    const staleSock = join(busDir, "dead0002.sock");
    await makeStaleSocket(staleSock);
    writeEntry(busDir, { v: 1, id: "dead0002", sessionId: "s2", pid: 2, socket: staleSock, startedAt: "2026-10-02T10:00:01.000Z" });
    assert.ok(existsSync(staleSock));

    const peers = await listPeers(busDir, "00000000");
    assert.deepEqual(peers.map((p) => p.id), [el.id]);
    assert.equal(existsSync(join(busDir, "dead0001.json")), false);
    assert.equal(existsSync(join(busDir, "dead0002.json")), false);
    assert.equal(existsSync(staleSock), false);
    assert.equal(existsSync(join(busDir, `${el.id}.json`)), true);
  });

  it("does not prune when pruning is disabled", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    writeEntry(busDir, {
      v: 1, id: "dead0001", sessionId: "s1", pid: 1, socket: join(busDir, "dead0001.sock"), startedAt: "2026-10-02T10:00:00.000Z",
    });
    assert.deepEqual(await listPeers(busDir, undefined, { prune: false }), []);
    assert.equal(existsSync(join(busDir, "dead0001.json")), true);
  });

  it("never prunes on a timeout", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    const sock = join(busDir, "slow0001.sock");
    const silent = await listenSilent(sock);
    try {
      writeEntry(busDir, { v: 1, id: "slow0001", sessionId: "s", pid: 3, socket: sock, startedAt: "2026-10-02T10:00:00.000Z" });
      const started = Date.now();
      const peers = await listPeers(busDir, undefined, { helloTimeoutMs: 100 });
      assert.deepEqual(peers, []);
      assert.ok(Date.now() - started < 1500);
      assert.equal(existsSync(join(busDir, "slow0001.json")), true);
      assert.equal(existsSync(sock), true);
      const err = await rejection(helloProbe(sock, 50));
      assert.equal(err.code, "timeout");
      assert.equal(isStaleError(err), false);
    } finally {
      await silent.close();
    }
  });

  it("skips (without pruning) an entry whose socket answers with another id", async () => {
    const a = makeEndpoint({}, "session-A");
    const ea = await a.endpoint.start();
    writeEntry(busDir, { v: 1, id: "lie00001", sessionId: "s", pid: 4, socket: ea.socket, startedAt: "2026-10-02T10:00:00.000Z" });
    const peers = await listPeers(busDir, undefined);
    assert.deepEqual(peers.map((p) => p.id), [ea.id]);
    assert.equal(existsSync(join(busDir, "lie00001.json")), true);
  });

  it("sendNote to a dead socket is 'unreachable' with a stale errno", async () => {
    const dead = join(tmp, "dead.sock");
    const err = await rejection(sendNote(dead, noteTo("00000000")));
    assert.equal(err.code, "unreachable");
    assert.equal(err.errno, "ENOENT");
    assert.equal(isStaleError(err), true);
  });

  it("sendNote times out against a silent peer", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    const silent = await listenSilent(join(busDir, "slow0002.sock"));
    try {
      const err = await rejection(sendNote(join(busDir, "slow0002.sock"), noteTo("00000000"), 100));
      assert.equal(err.code, "timeout");
    } finally {
      await silent.close();
    }
  });
});

describe("EADDRINUSE recovery", () => {
  it("reclaims a stale socket left by a dead process (same id)", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    const { endpoint } = makeEndpoint({ id: "5eed0001" });
    const stale = join(busDir, "5eed0001.sock");
    await makeStaleSocket(stale);
    assert.ok(existsSync(stale));
    const entry = await endpoint.start();
    assert.equal(entry.id, "5eed0001");
    assert.equal((await helloProbe(entry.socket)).id, "5eed0001");
    assert.equal(mode(entry.socket), 0o600);
  });

  it("reclaims a leftover regular file at the socket path", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    writeFileSync(join(busDir, "5eed0002.sock"), "junk");
    const { endpoint } = makeEndpoint({ id: "5eed0002" });
    assert.equal((await endpoint.start()).id, "5eed0002");
  });

  it("picks another id when a live endpoint holds the first choice, and leaves it untouched", async () => {
    const first = makeEndpoint({ id: "11110000" }, "session-A");
    const second = makeEndpoint({ id: "11110000" }, "session-B");
    const e1 = await first.endpoint.start();
    const e2 = await second.endpoint.start();
    assert.equal(e1.id, "11110000");
    assert.notEqual(e2.id, "11110000");
    assert.match(e2.id, /^[0-9a-f]{8}$/);
    assert.equal((await helloProbe(e1.socket)).sessionId, "session-A");
    assert.equal((await helloProbe(e2.socket)).sessionId, "session-B");
    const peers = await listPeers(busDir, undefined);
    assert.equal(peers.length, 2);
    // notes still reach the right endpoint
    await sendNote(e2.socket, noteTo(e2.id));
    assert.equal(second.notes.length, 1);
    assert.equal(first.notes.length, 0);
  });

  it("treats an unresponsive listener as live (does not steal its socket)", async () => {
    mkdirSync(busDir, { mode: 0o700, recursive: true });
    const silent = await listenSilent(join(busDir, "22220000.sock"));
    try {
      const { endpoint } = makeEndpoint({ id: "22220000", probeTimeoutMs: 100 });
      const entry = await endpoint.start();
      assert.notEqual(entry.id, "22220000");
      assert.equal(existsSync(join(busDir, "22220000.sock")), true);
    } finally {
      await silent.close();
    }
  });
});

describe("shutdown", () => {
  it("removes socket and registry entry, is idempotent, and restores the exit listeners", async () => {
    const before = process.listenerCount("exit");
    const { endpoint } = makeEndpoint();
    const entry = await endpoint.start();
    assert.equal(process.listenerCount("exit"), before + 1);
    await endpoint.stop();
    assert.equal(existsSync(entry.socket), false);
    assert.equal(existsSync(join(busDir, `${entry.id}.json`)), false);
    assert.equal(process.listenerCount("exit"), before);
    assert.equal(endpoint.running, false);
    await endpoint.stop(); // second call is a no-op
    await Promise.all([endpoint.stop(), endpoint.stop()]);
    assert.equal(process.listenerCount("exit"), before);
    await assert.rejects(helloProbe(entry.socket), BusClientError);
    assert.deepEqual(readdirSync(busDir), []);
  });

  it("stop() without start() is harmless", async () => {
    const before = process.listenerCount("exit");
    const { endpoint } = makeEndpoint();
    await endpoint.stop();
    await endpoint.stop();
    assert.equal(process.listenerCount("exit"), before);
  });

  it("destroys open connections", async () => {
    const { endpoint } = makeEndpoint({ limits: { idleTimeoutMs: 10_000 } });
    const { socket } = await endpoint.start();
    const pending = rawExchange(socket, '{"v":1,"type":"hel', 3000);
    await new Promise((r) => setTimeout(r, 50));
    await endpoint.stop();
    const res = await pending;
    assert.equal(res.closed, true);
    assert.equal(res.timedOut, false);
  });

  it("answers in-flight requests with rejected 'shutting down'", async () => {
    let release: () => void = () => {};
    let entered: () => void = () => {};
    const inHandler = new Promise<void>((r) => (entered = r));
    const { endpoint } = makeEndpoint({
      onNote: () =>
        new Promise<NoteResponse>((resolve) => {
          entered();
          release = () => resolve(DELIVERED);
        }),
    });
    const entry = await endpoint.start();
    const sending = rejection(sendNote(entry.socket, noteTo(entry.id), 5000));
    await inHandler;
    await endpoint.stop();
    const err = await sending;
    assert.equal(err.code, "rejected");
    assert.equal(err.reason, "shutting down");
    release(); // late completion of the handler is harmless
    await new Promise((r) => setTimeout(r, 20));
  });

  it("leaves a newer endpoint's files alone when an old one stops late", async () => {
    const old = makeEndpoint({ id: "99990000" }, "session-A");
    const oldEntry = await old.endpoint.start();
    // Simulate a successor that rewrote the registry entry for the same id.
    writeEntry(busDir, { ...oldEntry, pid: oldEntry.pid + 1, startedAt: "2030-01-01T00:00:00.000Z" });
    await old.endpoint.stop();
    assert.equal(existsSync(join(busDir, "99990000.json")), true);
  });

  it("process 'exit' cleanup removes the files synchronously", async () => {
    const { endpoint } = makeEndpoint();
    const entry = await endpoint.start();
    const listeners = process.listeners("exit");
    const ours = listeners[listeners.length - 1] as () => void;
    ours(); // what Node would do at exit
    assert.equal(existsSync(join(busDir, `${entry.id}.json`)), false);
    assert.equal(existsSync(entry.socket), false);
  });
});

describe("long socket paths", () => {
  it("falls back to the private runtime dir when the bus path would exceed 103 bytes", async () => {
    const longBus = join(tmp, "l".repeat(100), "bus");
    const runtime = join(tmp, "run");
    mkdirSync(runtime, { mode: 0o755 });
    const { endpoint } = makeEndpoint({ busDir: longBus, socketPath: { env: { XDG_RUNTIME_DIR: runtime } } });
    const entry = await endpoint.start();
    const fallbackDir = join(runtime, `pi-session-bus-${process.getuid?.()}`);
    assert.equal(entry.socket, join(fallbackDir, `${entry.id}.sock`));
    assert.ok(Buffer.byteLength(entry.socket) <= 103);
    assert.equal(mode(fallbackDir), 0o700);
    assert.equal(mode(entry.socket), 0o600);
    // the registry stays in the bus dir and records the absolute fallback socket
    assert.equal(mode(join(longBus, `${entry.id}.json`)), 0o600);
    assert.equal(JSON.parse(readFileSync(join(longBus, `${entry.id}.json`), "utf8")).socket, entry.socket);
    assert.equal(existsSync(join(longBus, `${entry.id}.sock`)), false);

    assert.equal((await helloProbe(entry.socket)).id, entry.id);
    const peers = await listPeers(longBus, undefined);
    assert.deepEqual(peers.map((p) => p.socket), [entry.socket]);

    await endpoint.stop();
    assert.equal(existsSync(entry.socket), false);
    assert.equal(existsSync(join(longBus, `${entry.id}.json`)), false);
    assert.equal(lstatSync(fallbackDir).isDirectory(), true);
  });
});
