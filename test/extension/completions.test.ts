import assert from "node:assert/strict";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createSessionBusExtension, USAGE } from "../../src/index.ts";
import { BUS_COMPLETIONS, completeBusArguments, completeQueueArguments, QUEUE_COMPLETIONS } from "../../src/completions.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";
import { FakeHost } from "./helpers.ts";

const bus = (prefix: string) => completeBusArguments(prefix)?.map(item => item.value) ?? null;
const queue = (prefix: string) => completeQueueArguments(prefix)?.map(item => item.value) ?? null;

let tmp: string;
let hosts: FakeHost[];
beforeEach(() => { tmp = makeTempDir("sb-complete"); hosts = []; });
afterEach(async () => {
  for (const host of hosts) if (!host.dead) await host.shutdown();
  for (const host of hosts) assert.deepEqual(host.callsAfterDeath, []);
  removeTempDir(tmp);
});
async function host(): Promise<FakeHost> {
  const repo = join(tmp, "repo"), busDir = join(tmp, "bus");
  mkdirSync(repo, { recursive: true });
  const h = new FakeHost({ sessionId: `complete-session-${hosts.length + 1}`, cwd: repo });
  h.pi.getActiveTools = () => ["read"];
  h.pi.setActiveTools = () => {};
  createSessionBusExtension({ busDir, gitToplevel: async () => repo, queuePollMs: 20, queueImageDir: busDir })(h.pi);
  hosts.push(h); await h.start(); return h;
}

describe("/bus and /queue argument completions", () => {
  it("offer every subcommand on empty input, without duplicates", () => {
    assert.deepEqual(bus(""), BUS_COMPLETIONS.map(item => item.value));
    assert.deepEqual(queue(""), QUEUE_COMPLETIONS.map(item => item.value));
    for (const list of [bus("")!, queue("")!]) assert.equal(new Set(list).size, list.length);
  });

  it("complete the whole argument string, including nested choices", () => {
    assert.deepEqual(bus("w"), ["wake on", "wake off"]);
    assert.deepEqual(bus("wake of"), ["wake off"]);
    assert.deepEqual(bus("  se"), ["send "]);
    assert.deepEqual(queue("o"), ["on", "off"]);
    assert.deepEqual(queue("d"), ["done"]);
  });

  it("offer nothing for unknown tokens, a lone exact match or free text", () => {
    assert.equal(bus("xyz"), null);
    assert.equal(bus("list"), null);
    assert.equal(bus("wake on"), null);
    assert.equal(bus("send "), null);
    assert.equal(bus("send peer wake on"), null);
    assert.equal(bus("wake  o"), null);
    assert.equal(queue("done"), null);
    assert.equal(queue("fix the lint"), null);
    assert.equal(queue("only o"), null);
  });

  it("are wired to the registered commands, and each completed subcommand runs as a subcommand", async () => {
    const h = await host();
    assert.equal(h.commands.get("bus")!.getArgumentCompletions, completeBusArguments);
    assert.equal(h.commands.get("queue")!.getArgumentCompletions, completeQueueArguments);
    for (const { value } of BUS_COMPLETIONS.filter(item => !item.value.endsWith(" "))) {
      const before = h.notifications.length;
      await h.runCommand(value, "bus");
      assert.ok(!h.notifications.slice(before).some(n => n.message === USAGE), value);
    }
    for (const { value } of QUEUE_COMPLETIONS) await h.runCommand(value, "queue");
    assert.ok(h.notifications.some(n => n.message === "Repository queue mode is on."));
    assert.ok(h.notifications.some(n => n.message === "session-bus auto-wake is off for this session."));
    // A queue subcommand that was not recognised would have been sent as a prompt.
    assert.deepEqual(h.userMessages, []);
  });
});
