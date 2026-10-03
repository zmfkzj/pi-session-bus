import assert from "node:assert/strict";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { encodeFrame, FrameDecoder, socketFileName } from "../../src/core/index.ts";
import { createSessionBusExtension } from "../../src/index.ts";
import { listenWith, makeTempDir, peerInfo, removeTempDir, type TestServer } from "../core/helpers.ts";
import { FakeHost, startPeer, type Peer } from "./helpers.ts";

let tmp: string;
let busDir: string;
let host: FakeHost;
let peers: Peer[];
let servers: TestServer[];
beforeEach(async () => {
  tmp = makeTempDir("sb-send-cancel");
  busDir = join(tmp, "bus");
  mkdirSync(busDir, { mode: 0o700 });
  peers = [];
  servers = [];
  host = new FakeHost();
  createSessionBusExtension({ busDir, helloTimeoutMs: 2000, noteTimeoutMs: 2000 })(host.pi);
  await host.start();
});
afterEach(async () => {
  await host.shutdown();
  await Promise.all(peers.map(peer => peer.endpoint.stop()));
  await Promise.all(servers.map(server => server.close()));
  assert.deepEqual(host.callsAfterDeath, []);
  removeTempDir(tmp);
});

async function send(method: "tool" | "command", to: string, signal: AbortSignal) {
  if (method === "tool") {
    await host.tool("session_send").execute("cancel-call", { to, content: "cancel me" }, signal, undefined, host.ctx);
  } else {
    Object.defineProperty(host.ctx, "signal", { value: signal, configurable: true });
    await host.runCommand(`send ${to} cancel me`);
  }
}

async function expectFailure(method: "tool" | "command", sending: Promise<void>, message: RegExp) {
  if (method === "tool") await assert.rejects(sending, message);
  else {
    await sending;
    assert.equal(host.lastNotification?.level, "error");
    assert.match(host.lastNotification!.message, message);
  }
}

describe("session send cancellation", () => {
  for (const method of ["tool", "command"] as const) {
    it(`${method}: abort during hello discovery never delivers or wakes the target`, { timeout: 5000 }, async () => {
      const target = await startPeer(busDir, "target-session", { name: "target" });
      peers.push(target);
      let helloSeen!: () => void;
      const discovering = new Promise<void>(resolve => { helloSeen = resolve; });
      let answerHello!: () => void;
      const id = "abcdef12";
      servers.push(await listenWith(join(busDir, socketFileName(id, process.pid)), sock => {
        const decoder = new FrameDecoder();
        sock.on("data", chunk => {
          if (decoder.push(chunk).frames.length > 0) {
            answerHello = () => sock.end(encodeFrame({ v: 1, ok: true, peer: peerInfo(id, process.pid) }));
            helloSeen(); // Hold discovery open until the caller has cancelled.
          }
        });
      }));
      const controller = new AbortController();
      const sending = send(method, target.endpoint.id, controller.signal);
      const failure = expectFailure(method, sending, /cancelled before sending\./);
      await discovering;
      controller.abort();
      answerHello();
      await failure;
      assert.deepEqual(target.notes, [], "no note means onNote cannot wake the target");
    });

    it(`${method}: abort while waiting for acknowledgement warns that delivery is unconfirmed`, { timeout: 5000 }, async () => {
      const controller = new AbortController();
      let release!: () => void;
      const holdResponse = new Promise<void>(resolve => { release = resolve; });
      const target = await startPeer(busDir, "target-session", {
        onNote: async () => {
          controller.abort();
          await holdResponse;
          return { v: 1, ok: true, status: "delivered", wake: "started" };
        },
      });
      peers.push(target);
      try {
        await expectFailure(method, send(method, target.endpoint.id, controller.signal),
          /cancelled; delivery is unconfirmed.*Do not resend blindly/);
        assert.equal(target.notes.length, 1, "cancellation cannot undo a frame the peer already received");
      } finally {
        release();
      }
    });
  }
});
