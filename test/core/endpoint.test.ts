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
  utimesSync,
  writeFileSync,
} from "node:fs";
import { createConnection } from "node:net";
import { basename, dirname, join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  BusClientError,
  createEndpoint,
  helloProbe,
  isStaleError,
  listPeers,
  sendNote,
  sendQueueNudge,
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
import { fallbackDir, socketFileName } from "../../src/core/registry.ts";
import {
  findDeadPid,
  listenHello,
  listenSilent,
  listenWith,
  makeKilledChildSocket,
  makeStaleSocket,
  makeTempDir,
  peerInfo,
  rawExchange,
  removeTempDir,
  type TestServer,
} from "./helpers.ts";

const mode = (path: string): number => statSync(path).mode & 0o777;
const DELIVERED: NoteResponse = { v: 1, ok: true, status: "delivered", wake: "started" };

let tmp: string;
let busDir: string;
let endpoints: Endpoint[];
let servers: TestServer[];

beforeEach(() => {
  tmp = makeTempDir("sb-ep");
  busDir = join(tmp, "bus");
  endpoints = [];
  servers = [];
});

afterEach(async () => {
  await Promise.all(endpoints.map((e) => e.stop()));
  await Promise.all(servers.map((s) => s.close()));
  removeTempDir(tmp);
});

/** `<busDir>/<id>-<pid>.sock` (default: this process, which is alive). */
const sock = (id: string, pid: number = process.pid): string => join(busDir, socketFileName(id, pid));
const ensureBus = (): void => {
  mkdirSync(busDir, { mode: 0o700, recursive: true });
};
/** Register a test server so afterEach closes it. */
async function serve(server: Promise<TestServer>): Promise<TestServer> {
  const s = await server;
  servers.push(s);
  return s;
}

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
  it("creates a 0700 bus dir and a 0600 socket named <id>-<pid>.sock, and nothing else", async () => {
    const { endpoint } = makeEndpoint({ pid: 31337 });
    const entry = await endpoint.start();
    assert.equal(endpoint.running, true);
    assert.deepEqual(endpoint.info, entry);
    assert.match(entry.id, /^[0-9a-f]{8}$/);
    assert.equal(entry.sessionId, "session-A");
    assert.equal(entry.pid, 31337);
    assert.equal(entry.socket, join(busDir, `${entry.id}-31337.sock`));
    assert.ok(!Number.isNaN(Date.parse(entry.startedAt)));

    assert.equal(mode(busDir), 0o700);
    assert.equal(mode(entry.socket), 0o600);
    assert.ok(statSync(entry.socket).isSocket());
    assert.deepEqual(readdirSync(busDir), [`${entry.id}-31337.sock`]); // the socket is the whole registry: no .json
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
    // (fake pids: only the ids matter here; b's start sweeps a's socket because pid 1001 does not exist)
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

describe("listPeers", () => {
  it("lists live peers, excludes self, and reports live data", async () => {
    const a = makeEndpoint({}, "session-A");
    const b = makeEndpoint({ getPeerInfo: () => ({ name: "beta", cwd: "/work/b", busy: true, autoWake: false }) }, "session-B");
    const ea = await a.endpoint.start();
    const eb = await b.endpoint.start();
    const fromA = await listPeers(busDir, ea.id);
    assert.deepEqual(
      fromA.map((p) => [p.id, p.name, p.busy, p.autoWake, p.socket, p.pid]),
      [[eb.id, "beta", true, false, eb.socket, process.pid]],
    );
    const fromNobody = await listPeers(busDir, undefined);
    assert.deepEqual(fromNobody.map((p) => p.id).sort(), [ea.id, eb.id].sort());
  });

  it("reports the socket's mtime as startedAt and lists the oldest first", async () => {
    const [ea, eb, ec] = [
      await makeEndpoint({}, "session-A").endpoint.start(),
      await makeEndpoint({}, "session-B").endpoint.start(),
      await makeEndpoint({}, "session-C").endpoint.start(),
    ];
    utimesSync(ea.socket, 3000, 3000);
    utimesSync(eb.socket, 1000, 1000);
    utimesSync(ec.socket, 2000, 2000);
    const peers = await listPeers(busDir, undefined);
    assert.deepEqual(peers.map((p) => p.id), [eb.id, ec.id, ea.id]);
    assert.deepEqual(peers.map((p) => p.startedAt), [
      "1970-01-01T00:16:40.000Z",
      "1970-01-01T00:33:20.000Z",
      "1970-01-01T00:50:00.000Z",
    ]);
  });

  it("(a) removes the socket of a SIGKILLed child process (its pid is gone) and does not list it", async () => {
    const live = makeEndpoint({}, "session-live");
    const el = await live.endpoint.start();
    const dead = await makeKilledChildSocket(busDir, "dead0001"); // named <id>-<child pid>.sock
    assert.equal(basename(dead.path), `dead0001-${dead.pid}.sock`);
    assert.equal(lstatSync(dead.path).isSocket(), true);
    assert.throws(() => process.kill(dead.pid, 0), { code: "ESRCH" });

    const peers = await listPeers(busDir, "00000000");
    assert.deepEqual(peers.map((p) => p.id), [el.id]);
    assert.equal(existsSync(dead.path), false);
    assert.equal(existsSync(el.socket), true);
  });

  it("removes a stale socket whose pid is alive when connecting is refused (ECONNREFUSED)", async () => {
    ensureBus();
    const stale = sock("dead0002"); // this process: alive, so the pid check cannot decide
    await makeStaleSocket(stale); // bound by a SIGKILLed child: the file stays, nobody listens
    assert.equal(process.kill(process.pid, 0), true);
    const err = await rejection(helloProbe(stale, 500));
    assert.equal(err.errno, "ECONNREFUSED");
    assert.equal(isStaleError(err), true);
    assert.ok(existsSync(stale));

    assert.deepEqual(await listPeers(busDir, undefined), []);
    assert.equal(existsSync(stale), false);
  });

  it("(b) removes a socket named with a nonexistent pid without connecting to it", async () => {
    ensureBus();
    const pid = findDeadPid(); // 2^22-1 (or the next unused pid)
    assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    const path = sock("dead0003", pid);
    const server = await serve(listenSilent(path)); // a live listener: only a connect attempt could tell
    assert.deepEqual(await listPeers(busDir, undefined, { helloTimeoutMs: 50 }), []);
    assert.equal(existsSync(path), false);
    assert.equal(server.connections(), 0, "nobody connected");
  });

  it("(c) skips a live server whose hello answers another id or pid than its file name, and does not delete it", async () => {
    ensureBus();
    const match = await serve(listenHello(sock("c0de0001"), peerInfo("c0de0001", process.pid))); // control
    const wrongId = await serve(listenHello(sock("c0de0002"), peerInfo("c0de00ff", process.pid)));
    const wrongPid = await serve(listenHello(sock("c0de0003"), peerInfo("c0de0003", process.pid + 1)));

    const peers = await listPeers(busDir, undefined);
    assert.deepEqual(peers.map((p) => [p.id, p.pid, p.socket]), [["c0de0001", process.pid, sock("c0de0001")]]);
    for (const id of ["c0de0001", "c0de0002", "c0de0003"]) assert.equal(existsSync(sock(id)), true, id);
    assert.equal(match.connections(), 1);
    assert.equal(wrongId.connections(), 1); // it was probed, answered, and skipped
    assert.equal(wrongPid.connections(), 1);
  });

  it("(d) never deletes a regular file, symlink or directory named like a socket (listing and start sweep)", async () => {
    ensureBus();
    const dead = findDeadPid();
    const target = join(tmp, "target.sock");
    await makeStaleSocket(target); // a real stale socket, reachable only through a symlink
    const regularDead = sock("f11e0001", dead);
    const regularAlive = sock("f11e0002");
    const directory = sock("f11e0003", dead);
    const dangling = sock("f11e0004", dead);
    const linked = sock("f11e0005");
    writeFileSync(regularDead, "keep1");
    writeFileSync(regularAlive, "keep2");
    mkdirSync(directory);
    symlinkSync(join(tmp, "nowhere"), dangling);
    symlinkSync(target, linked);
    const check = (when: string): void => {
      assert.equal(readFileSync(regularDead, "utf8"), "keep1", when);
      assert.equal(readFileSync(regularAlive, "utf8"), "keep2", when);
      assert.equal(lstatSync(directory).isDirectory(), true, when);
      assert.equal(lstatSync(dangling).isSymbolicLink(), true, when);
      assert.equal(lstatSync(linked).isSymbolicLink(), true, when);
      assert.equal(lstatSync(target).isSocket(), true, when);
    };

    const live = makeEndpoint({}, "session-live");
    const el = await live.endpoint.start(); // the sweep runs here
    check("after the start sweep");
    assert.deepEqual((await listPeers(busDir, undefined)).map((p) => p.id), [el.id]);
    check("after listPeers");
  });

  it("does not prune when pruning is disabled (not even legacy files)", async () => {
    ensureBus();
    const dead = await makeKilledChildSocket(busDir, "dead0004");
    const aliveStale = sock("dead0005");
    await makeStaleSocket(aliveStale);
    const legacy = join(busDir, "1e9a0001.sock");
    await makeStaleSocket(legacy);
    writeFileSync(join(busDir, "1e9a0001.json"), "{}");
    assert.deepEqual(await listPeers(busDir, undefined, { prune: false }), []);
    for (const path of [dead.path, aliveStale, legacy, join(busDir, "1e9a0001.json")]) assert.equal(existsSync(path), true, path);
  });

  it("never prunes on a timeout", async () => {
    ensureBus();
    const sockPath = sock("slow0001");
    await serve(listenSilent(sockPath));
    const started = Date.now();
    const peers = await listPeers(busDir, undefined, { helloTimeoutMs: 100 });
    assert.deepEqual(peers, []);
    assert.ok(Date.now() - started < 1500);
    assert.equal(existsSync(sockPath), true);
    const err = await rejection(helloProbe(sockPath, 50));
    assert.equal(err.code, "timeout");
    assert.equal(isStaleError(err), false);
  });

  it("never prunes a peer that rejects the hello", async () => {
    ensureBus();
    const path = sock("bad00001");
    const server = await serve(
      listenWith(path, (s) => {
        s.on("data", () => s.end(encodeFrame({ v: 1, ok: false, status: "rejected", reason: "nope" })));
      }),
    );
    assert.deepEqual(await listPeers(busDir, undefined), []);
    assert.equal(existsSync(path), true);
    assert.equal(server.connections(), 1);
  });

  it("sendNote to a dead socket is 'unreachable' with a stale errno", async () => {
    const dead = join(tmp, "dead.sock");
    const err = await rejection(sendNote(dead, noteTo("00000000")));
    assert.equal(err.code, "unreachable");
    assert.equal(err.errno, "ENOENT");
    assert.equal(isStaleError(err), true);
  });

  it("sendNote times out against a silent peer", async () => {
    ensureBus();
    await serve(listenSilent(sock("slow0002")));
    const err = await rejection(sendNote(sock("slow0002"), noteTo("00000000"), 100));
    assert.equal(err.code, "timeout");
  });
});

describe("legacy cleanup (previous <id>.sock + <id>.json layout)", () => {
  const legacySock = (id: string): string => join(busDir, `${id}.sock`);
  const legacyJson = (id: string): string => join(busDir, `${id}.json`);

  it("(e) removes a legacy socket nobody listens on together with its json when listing", async () => {
    ensureBus();
    await makeStaleSocket(legacySock("1e9a0001"));
    writeFileSync(legacyJson("1e9a0001"), JSON.stringify({ v: 1, id: "1e9a0001" }));
    const live = makeEndpoint({}, "session-live");
    const el = await live.endpoint.start(); // start does not touch legacy files (the sweep only drops sockets of vanished pids)
    assert.equal(existsSync(legacySock("1e9a0001")), true);
    assert.equal(existsSync(legacyJson("1e9a0001")), true);

    const peers = await listPeers(busDir, undefined);
    assert.deepEqual(peers.map((p) => p.id), [el.id]);
    assert.equal(existsSync(legacySock("1e9a0001")), false);
    assert.equal(existsSync(legacyJson("1e9a0001")), false);
    assert.equal(existsSync(el.socket), true);
  });

  it("leaves a live legacy session alone (socket and json) and does not list it", async () => {
    ensureBus();
    const server = await serve(listenHello(legacySock("1e9a0002"), peerInfo("1e9a0002", process.pid)));
    writeFileSync(legacyJson("1e9a0002"), "{}");
    assert.deepEqual(await listPeers(busDir, undefined), []);
    assert.equal(server.connections(), 1); // it was probed
    assert.equal(existsSync(legacySock("1e9a0002")), true);
    assert.equal(existsSync(legacyJson("1e9a0002")), true);
  });

  it("leaves a legacy socket alone when the probe times out", async () => {
    ensureBus();
    await serve(listenSilent(legacySock("1e9a0003")));
    writeFileSync(legacyJson("1e9a0003"), "{}");
    assert.deepEqual(await listPeers(busDir, undefined, { helloTimeoutMs: 100 }), []);
    assert.equal(existsSync(legacySock("1e9a0003")), true);
    assert.equal(existsSync(legacyJson("1e9a0003")), true);
  });

  it("only touches sockets: a regular file named <id>.sock stays, with its json", async () => {
    ensureBus();
    writeFileSync(legacySock("1e9a0004"), "junk");
    writeFileSync(legacyJson("1e9a0004"), "{}");
    assert.deepEqual(await listPeers(busDir, undefined), []);
    assert.equal(readFileSync(legacySock("1e9a0004"), "utf8"), "junk");
    assert.equal(existsSync(legacyJson("1e9a0004")), true);
  });

  it("never lists a legacy json, with or without a socket", async () => {
    ensureBus();
    writeFileSync(legacyJson("1e9a0005"), JSON.stringify({ v: 1, id: "1e9a0005", sessionId: "s", pid: process.pid, socket: legacySock("1e9a0005"), startedAt: "2026-10-02T10:00:00.000Z" }));
    assert.deepEqual(await listPeers(busDir, undefined), []);
  });
});

describe("sweep after start", () => {
  it("removes sockets of vanished pids, keeps everything that is not provably dead, and never connects", async () => {
    ensureBus();
    const deadPid = findDeadPid();
    const killed = await makeKilledChildSocket(busDir, "dead0001"); // pid gone
    const ghost = await serve(listenSilent(sock("dead0002", deadPid))); // pid gone, but something listens
    const aliveStale = sock("dead0003"); // pid alive, nobody listens: unknown, so it stays
    await makeStaleSocket(aliveStale);
    const alive = await serve(listenSilent(sock("dead0004")));
    writeFileSync(sock("dead0005", deadPid), "junk"); // not a socket
    writeFileSync(join(busDir, "notes.txt"), "keep");

    const info = await makeEndpoint({}, "session-new").endpoint.start();

    assert.equal(existsSync(killed.path), false);
    assert.equal(existsSync(sock("dead0002", deadPid)), false);
    assert.equal(existsSync(aliveStale), true);
    assert.equal(existsSync(sock("dead0004")), true);
    assert.equal(readFileSync(sock("dead0005", deadPid), "utf8"), "junk");
    assert.equal(readFileSync(join(busDir, "notes.txt"), "utf8"), "keep");
    assert.equal(existsSync(info.socket), true);
    assert.equal(ghost.connections(), 0, "the sweep never connects");
    assert.equal(alive.connections(), 0, "the sweep never connects");
  });

  it("never removes the socket of the endpoint that is starting", async () => {
    const pid = findDeadPid(); // our own pid is "gone" as far as the file name is concerned
    const { endpoint } = makeEndpoint({ pid });
    const info = await endpoint.start();
    assert.equal(info.socket, sock(info.id, pid));
    assert.equal(existsSync(info.socket), true);
    assert.equal((await helloProbe(info.socket)).pid, pid);
  });

  it("never deletes a .json at start: orphan or not, legacy files are only cleaned up together with a stale busDir socket", async () => {
    ensureBus();
    writeFileSync(join(busDir, "1e9a0001.json"), "{}"); // no socket at all
    await serve(listenSilent(join(busDir, "1e9a0002.sock"))); // old session still running
    writeFileSync(join(busDir, "1e9a0002.json"), "{}");
    writeFileSync(join(busDir, "notes.json"), "{}");
    writeFileSync(join(busDir, "1E9A0003.json"), "{}"); // not an id
    await makeEndpoint({}, "session-new").endpoint.start();
    for (const name of ["1e9a0001.json", "1e9a0002.json", "1e9a0002.sock", "notes.json", "1E9A0003.json"]) {
      assert.equal(existsSync(join(busDir, name)), true, `${name} after start`);
    }
    // listing removes a json only together with the stale busDir socket of the same id, so these stay too
    await listPeers(busDir, undefined, { helloTimeoutMs: 100 });
    for (const name of ["1e9a0001.json", "1e9a0002.json", "1e9a0002.sock", "notes.json", "1E9A0003.json"]) {
      assert.equal(existsSync(join(busDir, name)), true, `${name} after listPeers`);
    }
  });

  it("keeps the .json of a live old session whose socket sits in the old fallback dir (<runtime>/pi-session-bus-<uid>/<id>.sock)", async () => {
    ensureBus();
    const runtime = join(tmp, "run");
    const socketPath = { env: { XDG_RUNTIME_DIR: runtime } };
    // previous layout: json in the bus dir, socket directly in <runtime>/pi-session-bus-<uid>/ (no hash level)
    const oldDir = join(runtime, `pi-session-bus-${process.getuid?.()}`);
    mkdirSync(oldDir, { recursive: true, mode: 0o700 });
    const oldSocket = join(oldDir, "1e9a0006.sock");
    const oldJson = join(busDir, "1e9a0006.json");
    await serve(listenHello(oldSocket, peerInfo("1e9a0006", process.pid)));
    writeFileSync(oldJson, JSON.stringify({ v: 1, id: "1e9a0006", sessionId: "old", pid: process.pid, socket: oldSocket, startedAt: "2026-10-02T10:00:00.000Z" }));

    const info = await makeEndpoint({ socketPath }, "session-new").endpoint.start(); // the start sweep runs here
    assert.equal(existsSync(oldJson), true, "json survives start()");
    assert.equal(existsSync(oldSocket), true);

    const peers = await listPeers(busDir, undefined, { socketPath });
    assert.deepEqual(peers.map((p) => p.id), [info.id], "the old session is not listed");
    assert.equal(existsSync(oldJson), true, "json survives listPeers");
    assert.equal(existsSync(oldSocket), true);
  });
});

describe("EADDRINUSE recovery", () => {
  it("reclaims a stale socket left by a dead process (same <id>-<pid> name)", async () => {
    ensureBus();
    const { endpoint } = makeEndpoint({ id: "5eed0001" });
    const stale = sock("5eed0001");
    await makeStaleSocket(stale);
    assert.ok(existsSync(stale));
    const entry = await endpoint.start();
    assert.equal(entry.id, "5eed0001");
    assert.equal(entry.socket, stale);
    assert.equal((await helloProbe(entry.socket)).id, "5eed0001");
    assert.equal(mode(entry.socket), 0o600);
  });

  it("reclaims a stale socket even when the pid in its name is gone", async () => {
    ensureBus();
    const pid = findDeadPid();
    const { endpoint } = makeEndpoint({ id: "5eed0003", pid });
    const stale = sock("5eed0003", pid);
    await makeStaleSocket(stale);
    const entry = await endpoint.start();
    assert.equal(entry.id, "5eed0003");
    assert.equal(entry.socket, stale);
    assert.equal((await helloProbe(entry.socket)).pid, pid);
  });

  it("keeps a regular file at the socket path (only sockets are unlinked) and salts the id instead", async () => {
    ensureBus();
    const path = sock("5eed0002");
    writeFileSync(path, "junk");
    const { endpoint } = makeEndpoint({ id: "5eed0002" });
    const entry = await endpoint.start();
    assert.notEqual(entry.id, "5eed0002");
    assert.equal(entry.socket, sock(entry.id));
    assert.equal(readFileSync(path, "utf8"), "junk");
    assert.equal((await helloProbe(entry.socket)).id, entry.id);
  });

  it("picks another id when a live endpoint holds the first choice, and leaves it untouched", async () => {
    const first = makeEndpoint({ id: "11110000" }, "session-A");
    const second = makeEndpoint({ id: "11110000" }, "session-B");
    const e1 = await first.endpoint.start();
    const e2 = await second.endpoint.start();
    assert.equal(e1.id, "11110000");
    assert.notEqual(e2.id, "11110000");
    assert.match(e2.id, /^[0-9a-f]{8}$/);
    assert.equal(e2.socket, sock(e2.id));
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
    ensureBus();
    const silent = await serve(listenSilent(sock("22220000")));
    const { endpoint } = makeEndpoint({ id: "22220000", probeTimeoutMs: 100 });
    const entry = await endpoint.start();
    assert.notEqual(entry.id, "22220000");
    assert.equal(existsSync(sock("22220000")), true);
    assert.equal(silent.connections(), 1); // only the liveness probe
  });
});

describe("shutdown", () => {
  it("removes its socket (and leaves nothing else), is idempotent, and restores the exit listeners", async () => {
    const before = process.listenerCount("exit");
    const { endpoint } = makeEndpoint();
    const entry = await endpoint.start();
    assert.equal(process.listenerCount("exit"), before + 1);
    await endpoint.stop();
    assert.equal(existsSync(entry.socket), false);
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

  it("unlinks only its own socket: other sockets and files stay", async () => {
    const a = makeEndpoint({}, "session-A");
    const b = makeEndpoint({}, "session-B");
    const ea = await a.endpoint.start();
    const eb = await b.endpoint.start();
    writeFileSync(join(busDir, "notes.txt"), "keep");
    const lookalike = sock("5afe0001", findDeadPid()); // a regular file named like a socket
    writeFileSync(lookalike, "keep");
    await a.endpoint.stop();
    assert.equal(existsSync(ea.socket), false);
    assert.equal((await helloProbe(eb.socket)).id, eb.id);
    assert.deepEqual(readdirSync(busDir).sort(), [basename(eb.socket), basename(lookalike), "notes.txt"].sort());
  });

  it("process 'exit' cleanup removes the socket synchronously", async () => {
    const { endpoint } = makeEndpoint();
    const entry = await endpoint.start();
    const listeners = process.listeners("exit");
    const ours = listeners[listeners.length - 1] as () => void;
    ours(); // what Node would do at exit
    assert.equal(existsSync(entry.socket), false);
    assert.deepEqual(readdirSync(busDir), []);
  });
});

describe("long socket paths", () => {
  const uid = process.getuid?.();

  it("falls back to <runtime>/pi-session-bus-<uid>/<hash of the bus dir> when the bus path would exceed 103 bytes", async () => {
    const longBus = join(tmp, "l".repeat(100), "bus");
    const runtime = join(tmp, "run");
    mkdirSync(runtime, { mode: 0o755 });
    const socketPath = { env: { XDG_RUNTIME_DIR: runtime } };
    const { endpoint } = makeEndpoint({ busDir: longBus, socketPath });
    const info = await endpoint.start();
    const dir = fallbackDir(longBus, socketPath);
    assert.equal(info.socket, join(dir, `${info.id}-${process.pid}.sock`));
    assert.equal(dirname(dir), join(runtime, `pi-session-bus-${uid}`));
    assert.match(basename(dir), /^[0-9a-f]{8}$/);
    assert.ok(Buffer.byteLength(info.socket) <= 103);
    assert.equal(mode(dirname(dir)), 0o700);
    assert.equal(mode(dir), 0o700);
    assert.equal(mode(info.socket), 0o600);
    // the socket is the only file anywhere: nothing (no .json) in the bus dir, nothing else in the fallback dir
    assert.deepEqual(readdirSync(longBus), []);
    assert.deepEqual(readdirSync(dir), [`${info.id}-${process.pid}.sock`]);

    assert.equal((await helloProbe(info.socket)).id, info.id);
    const peers = await listPeers(longBus, undefined, { socketPath });
    assert.deepEqual(peers.map((p) => p.socket), [info.socket]);
    // another bus dir has another hash: it does not see this endpoint
    assert.deepEqual(await listPeers(join(tmp, "m".repeat(100), "bus"), undefined, { socketPath }), []);

    await endpoint.stop();
    assert.equal(existsSync(info.socket), false);
    assert.equal(lstatSync(dir).isDirectory(), true);
  });

  it("sweeps and prunes dead sockets in the fallback dir too", async () => {
    const longBus = join(tmp, "l".repeat(100), "bus");
    const runtime = join(tmp, "run");
    mkdirSync(runtime, { mode: 0o755 });
    const socketPath = { env: { XDG_RUNTIME_DIR: runtime } };
    const first = await makeEndpoint({ busDir: longBus, socketPath }, "session-A").endpoint.start();
    const dir = dirname(first.socket);

    const swept = await makeKilledChildSocket(dir, "dead0001");
    assert.equal(existsSync(swept.path), true);
    const second = await makeEndpoint({ busDir: longBus, socketPath }, "session-B").endpoint.start(); // start sweeps
    assert.equal(existsSync(swept.path), false);

    const pruned = await makeKilledChildSocket(dir, "dead0002");
    const peers = await listPeers(longBus, undefined, { socketPath });
    assert.deepEqual(peers.map((p) => p.id).sort(), [first.id, second.id].sort());
    assert.equal(existsSync(pruned.path), false);
  });
});

describe("queue nudge endpoint", () => {
  it("accepts and dispatches content-free nudges through sendQueueNudge", async () => {
    const keys: string[] = [];
    const { endpoint, notes } = makeEndpoint({ onQueueNudge: (key) => { keys.push(key); } });
    const info = await endpoint.start();
    assert.deepEqual(await sendQueueNudge(info.socket, "0123456789abcdef"), { v: 1, ok: true, status: "accepted" });
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(keys, ["0123456789abcdef"]);
    assert.deepEqual(notes, []);
  });
  it("rejects nudges when the endpoint does not support them", async () => {
    const { endpoint } = makeEndpoint();
    const info = await endpoint.start();
    await assert.rejects(sendQueueNudge(info.socket, "0123456789abcdef"), (err: unknown) =>
      err instanceof BusClientError && err.code === "rejected" && !isStaleError(err));
  });
  it("rejects malformed queue keys without dispatching them", async () => {
    const keys: string[] = [];
    const { endpoint } = makeEndpoint({ onQueueNudge: (key) => { keys.push(key); } });
    const info = await endpoint.start();
    const result = await rawExchange(info.socket, encodeFrame({ v: 1, type: "queue_nudge", queue: "../escape" }));
    assert.equal(JSON.parse(result.received).ok, false);
    assert.deepEqual(keys, []);
    await assert.rejects(sendQueueNudge(info.socket, "bad"), BusClientError);
  });
  it("acknowledges before an asynchronous handler finishes and contains callback errors", async () => {
    let resolveHandler!: () => void;
    const pending = new Promise<void>((resolve) => { resolveHandler = resolve; });
    const { endpoint } = makeEndpoint({ onQueueNudge: () => pending });
    const info = await endpoint.start();
    assert.equal((await sendQueueNudge(info.socket, "0123456789abcdef", 50)).ok, true);
    resolveHandler();
    const throwing = makeEndpoint({ onQueueNudge: () => { throw new Error("test"); } }, "throwing");
    assert.equal((await sendQueueNudge((await throwing.endpoint.start()).socket, "0123456789abcdef")).ok, true);
  });
  it("preserves stale-error classification on nudge socket failures", async () => {
    await assert.rejects(sendQueueNudge(join(tmp, "missing.sock"), "0123456789abcdef"), isStaleError);
  });
});
