import assert from "node:assert/strict";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createQueueStore, deriveId, helloProbe, listSockets } from "../../src/core/index.ts";
import { createSessionBusExtension } from "../../src/index.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";
import { FakeHost, startPeer, type Peer } from "./helpers.ts";

let tmp: string;
let busDir: string;
let repo: string;
let hosts: FakeHost[];
let peers: Peer[];
beforeEach(() => {
  tmp = makeTempDir("sb-name");
  busDir = join(tmp, "bus");
  repo = join(tmp, "repo");
  mkdirSync(repo);
  hosts = [];
  peers = [];
});
afterEach(async () => {
  await Promise.all(hosts.map(host => host.shutdown()));
  await Promise.all(peers.map(peer => peer.endpoint.stop()));
  for (const host of hosts) assert.deepEqual(host.callsAfterDeath, []);
  removeTempDir(tmp);
});

const cases = [
  { title: "300 ASCII code units", name: "a".repeat(300), expected: "a".repeat(256) },
  { title: "astral character straddling the cap", name: "a".repeat(255) + "😀" + "b".repeat(43), expected: "a".repeat(255) },
  { title: "astral character ending at the cap", name: "a".repeat(254) + "😀" + "b".repeat(44), expected: "a".repeat(254) + "😀" },
];

describe("long advertised session names", () => {
  for (const { title, name, expected } of cases) {
    for (const method of ["tool", "command"] as const) {
      it(`${method}: ${title} sends an accepted note and uses the same name everywhere`, async () => {
        assert.equal(name.length, 300);
        const host = new FakeHost({ sessionName: name, cwd: repo });
        hosts.push(host);
        createSessionBusExtension({ busDir, gitToplevel: () => repo, queuePollMs: 60_000 })(host.pi);
        await host.start();
        const target = await startPeer(busDir, "target-session", { name: "target" });
        peers.push(target);

        if (method === "tool") {
          const result = await host.runTool("session_send", { to: "target", content: "long-name note" });
          assert.equal(result.details["status"], "delivered");
        } else {
          await host.runCommand("send target long-name note");
          assert.equal(host.lastNotification?.level, "info");
          assert.match(host.lastNotification!.message, /^Sent msg /);
        }
        assert.equal(target.notes.length, 1);
        assert.equal(target.notes[0]!.from.name, expected);
        assert.ok(target.notes[0]!.from.name!.length <= 256);
        assert.doesNotMatch(target.notes[0]!.from.name!, /[\uD800-\uDFFF]/u);
        assert.equal(host.sessionName, name, "the user's actual session name is not changed");

        const selfId = deriveId(host.ctx.sessionManager.getSessionId(), process.pid);
        const socket = listSockets(busDir).find(entry => entry.id === selfId)!;
        assert.equal((await helloProbe(socket.path)).name, expected);
        const listed = await host.runTool("session_list", {});
        assert.equal((listed.details["self"] as { name: string }).name, expected);
        await assert.rejects(host.runTool("session_send", { to: expected, content: "not to self" }), /this session itself/);
        assert.equal(target.notes.length, 1);

        await host.runCommand("work with a long session name", "queue");
        const entries = createQueueStore({ busDir, repo }).read().entries;
        assert.equal(entries.length, 1);
        assert.equal(entries[0]!.name, expected, "queue identity agrees with the wire identity");
      });
    }
  }

  it("a standalone endpoint also advertises a surrogate-safe hello name", async () => {
    const { name, expected } = cases[1]!;
    const target = await startPeer(busDir, "standalone-session", { name });
    peers.push(target);
    const hello = await helloProbe(target.endpoint.info!.socket);
    assert.equal(hello.name, expected);
    assert.doesNotMatch(hello.name!, /[\uD800-\uDFFF]/u);
  });
});
