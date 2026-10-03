import assert from "node:assert/strict";
import { getEventListeners } from "node:events";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { BusClientError, isStaleError, sendNote } from "../../src/core/endpoint.ts";
import { createNote, encodeFrame, FrameDecoder } from "../../src/core/protocol.ts";
import { listenWith, makeTempDir, removeTempDir, type TestServer } from "./helpers.ts";

let tmp: string;
let servers: TestServer[];
beforeEach(() => { tmp = makeTempDir("sb-cancel"); servers = []; });
afterEach(async () => {
  await Promise.all(servers.map(server => server.close()));
  removeTempDir(tmp);
});
const note = () => createNote({
  from: { id: "feedbeef", sessionId: "sender", cwd: "/sender", replyable: false },
  to: "deadbeef", content: "do not deliver after cancellation", hops: 1,
});
async function serve(onConnection: Parameters<typeof listenWith>[1]) {
  const path = join(tmp, "target.sock");
  const server = await listenWith(path, onConnection);
  servers.push(server);
  return { path, server };
}
const pause = () => new Promise(resolve => setTimeout(resolve, 20));

function cancelled(unconfirmed: boolean) {
  return (err: unknown): boolean => {
    assert.ok(err instanceof BusClientError);
    assert.equal(err.code, "cancelled");
    assert.equal(err.deliveryUnconfirmed, unconfirmed);
    assert.equal(isStaleError(err), false);
    if (unconfirmed) assert.match(err.message, /delivery is unconfirmed.*Do not resend blindly/);
    else assert.equal(err.message, "cancelled before sending.");
    return true;
  };
}

describe("sendNote cancellation", () => {
  it("an already aborted signal never connects", async () => {
    const { path, server } = await serve(() => {});
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(sendNote(path, note(), 1000, { signal: controller.signal }), cancelled(false));
    await pause();
    assert.equal(server.connections(), 0);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  it("cancellation before connect writes no frame and removes its listener", async () => {
    let bytes = 0;
    const { path } = await serve(sock => sock.on("data", chunk => { bytes += chunk.length; }));
    const controller = new AbortController();
    const sending = sendNote(path, note(), 1000, { signal: controller.signal });
    controller.abort();
    await assert.rejects(sending, cancelled(false));
    await pause();
    assert.equal(bytes, 0);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  it("abort after writing destroys the socket and reports unconfirmed delivery", { timeout: 2000 }, async () => {
    const controller = new AbortController();
    let closed!: () => void;
    const socketClosed = new Promise<void>(resolve => { closed = resolve; });
    let received = 0;
    const { path } = await serve(sock => {
      const decoder = new FrameDecoder();
      sock.on("close", closed);
      sock.on("data", chunk => {
        if (decoder.push(chunk).frames.length > 0) {
          received++;
          controller.abort(); // The note arrived, but its acknowledgement has not.
        }
      });
    });
    await assert.rejects(sendNote(path, note(), 1000, { signal: controller.signal }), cancelled(true));
    await socketClosed;
    assert.equal(received, 1);
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });

  it("successful exchanges remove the abort listener", async () => {
    const { path } = await serve(sock => {
      const decoder = new FrameDecoder();
      sock.on("data", chunk => {
        if (decoder.push(chunk).frames.length > 0) {
          sock.end(encodeFrame({ v: 1, ok: true, status: "delivered", wake: "started" }));
        }
      });
    });
    const controller = new AbortController();
    assert.equal((await sendNote(path, note(), 1000, { signal: controller.signal })).status, "delivered");
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
    controller.abort();
  });

  it("timed-out exchanges remove the abort listener", async () => {
    const { path } = await serve(() => {});
    const controller = new AbortController();
    await assert.rejects(sendNote(path, note(), 20, { signal: controller.signal }),
      (err: unknown) => err instanceof BusClientError && err.code === "timeout");
    assert.equal(getEventListeners(controller.signal, "abort").length, 0);
  });
});
