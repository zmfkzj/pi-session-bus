import assert from "node:assert/strict";
import { mkdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it, mock } from "node:test";
import { createSessionBusExtension, type SessionBusOptions } from "../../src/index.ts";
import { QUEUE_MESSAGE_TYPE } from "../../src/queue.ts";
import { createNote, createQueueStore, deriveId, listSockets, queueKey, sendNote, sendQueueNudge } from "../../src/core/index.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";
import { FakeHost, type FakeHostOptions } from "./helpers.ts";

let tmp: string;
let busDir: string;
let repo: string;
let hosts: FakeHost[];
let serial = 0;
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function waitFor(fn: () => boolean) {
  const until = Date.now() + 3000;
  while (!fn()) { if (Date.now() > until) throw new Error("queue check timed out"); await sleep(5); }
}
beforeEach(() => { tmp = makeTempDir("sb-q-wire"); busDir = join(tmp, "bus"); repo = join(tmp, "repo"); mkdirSync(repo); hosts = []; });
afterEach(async () => {
  for (const host of hosts) if (!host.dead) await host.shutdown();
  await sleep(20);
  for (const host of hosts) assert.deepEqual(host.callsAfterDeath, []);
  removeTempDir(tmp);
});
const activeTools = new WeakMap<FakeHost, { names: string[]; updates: string[][] }>();
function stubActiveTools(h: FakeHost, names = ["read", "bash", "edit"]) {
  const state = { names: [...names], updates: [] as string[][] };
  activeTools.set(h, state);
  h.pi.getActiveTools = () => [...state.names];
  h.pi.setActiveTools = names => { state.names = [...names]; state.updates.push([...names]); };
  return state;
}
async function host(opts: SessionBusOptions = {}, hostOpts: FakeHostOptions = {}) {
  const h = new FakeHost({ sessionId: `queue-session-${++serial}`, cwd: repo, sessionName: `worker-${serial}`, ...hostOpts });
  stubActiveTools(h);
  createSessionBusExtension({ busDir, gitToplevel: async () => repo, queuePollMs: 20, ...opts })(h.pi);
  hosts.push(h); await h.start(); return h;
}
const command = (h: FakeHost, text: string) => h.runCommand(text, "queue");
const input = (h: FakeHost, text: string, source = "interactive", images?: unknown[]) => h.fire("input", { text, source, ...(images ? { images } : {}) });
const store = () => createQueueStore({ busDir, repo });
const socket = (h: FakeHost) => listSockets(busDir).find(e => e.id === deriveId(h.ctx.sessionManager.getSessionId(), process.pid))!;
async function holding(h: FakeHost, text = "first task") { await command(h, text); h.idle = false; await h.fire("agent_start"); }
async function waiting(a: FakeHost, b: FakeHost) { await holding(a); await command(b, "on"); assert.deepEqual(await input(b, "B's task"), { action: "handled" }); }

describe("repository queue wiring", () => {
  it("/queue <prompt> runs immediately when free, expands templates and exposes an extension-authored turn notice", async () => {
    const a = await host({}, { mode: "tui" });
    await command(a, "work on {{task}}");
    assert.deepEqual(a.userMessages, [{ content: "work on {{task}}", options: { expandPromptTemplates: true } }]);
    assert.equal(store().read().entries[0]!.state, "active");
    const result = await a.fire("before_agent_start") as { message: { customType: string; content: string } };
    assert.equal(result.message.customType, QUEUE_MESSAGE_TYPE);
    assert.match(result.message.content, /holds the repository work-queue turn/);
    assert.ok(result.message.content.includes(repo));
    assert.match(result.message.content, /0 session\(s\) are waiting/);
    assert.match(result.message.content, /Call queue_done.*not asking or waiting/);
    assert.equal(await a.fire("before_agent_start"), undefined, "notice is only emitted once per acquisition");
    assert.equal(a.queueStatuses.at(-1), "queue: turn (idle)");
  });

  it("defers interactive and RPC input, replays on nudge exactly once with template expansion, and queues remaining prompts in order", async () => {
    const a = await host(); const b = await host({ queuePollMs: 60_000 });
    await waiting(a, b);
    assert.deepEqual(await input(b, "second", "rpc"), { action: "handled" });
    assert.deepEqual(await input(b, "third"), { action: "handled" });
    assert.equal(b.userMessages.length, 0);
    assert.match(b.lastNotification!.message, /queued #2 behind/);
    const result = await store().removeIfSameId(store().read().entries[0]!.id);
    assert.equal(result.promoted!.endpointId, socket(b).id);
    await Promise.all([sendQueueNudge(socket(b).path, queueKey(repo)), sendQueueNudge(socket(b).path, queueKey(repo))]);
    await waitFor(() => b.userMessages.length === 1);
    b.idle = false; await b.fire("agent_start");
    assert.deepEqual(b.userMessages.map(m => m.content), ["B's task", "second", "third"]);
    assert.ok(b.userMessages.every(m => m.options?.["expandPromptTemplates"] === true));
    assert.deepEqual(b.userMessages.slice(1).map(m => m.options?.["deliverAs"]), ["followUp", "followUp"]);
    await sendQueueNudge(socket(b).path, queueKey(repo)); await sleep(20);
    assert.equal(b.userMessages.length, 3);
  });

  it("ignores a nudge for another repository key", async () => {
    const a = await host(); const b = await host({ queuePollMs: 60_000 }); await waiting(a, b);
    await store().removeIfSameId(store().read().entries[0]!.id);
    await sendQueueNudge(socket(b).path, "0123456789abcdef"); await sleep(20);
    assert.equal(b.userMessages.length, 0);
  });

  it("polls a promotion without a nudge", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    await store().removeIfSameId(store().read().entries[0]!.id);
    await waitFor(() => b.userMessages.length === 1);
  });

  it("agent_start removes the expiry and agent_settled sets the default ten-minute grace", async () => {
    let clock = 1_700_000_000_000;
    const a = await host({ now: () => clock });
    await holding(a);
    assert.equal(store().read().entries[0]!.holdExpiresAt, null);
    clock += 10_000; a.idle = true; await a.fire("agent_settled");
    assert.equal(store().read().entries[0]!.holdExpiresAt, clock + 600_000);
  });

  it("a holder poll leaves its own expired idle entry for the idle timer to hand over", async () => {
    let clock = Date.now();
    const a = await host({ now: () => clock, queueIdleMs: 500, queuePollMs: 20 });
    const b = await host({ now: () => clock, queuePollMs: 60_000 });
    await waiting(a, b); a.idle = true; await a.fire("agent_settled");
    const id = store().read().entries[0]!.id;
    clock = store().read().entries[0]!.holdExpiresAt! + 1;
    await sleep(80); // Several holder polls, before the wall-clock idle timer fires.
    assert.equal(store().read().entries[0]!.id, id);
    assert.equal(b.userMessages.length, 0);
    assert.ok(!a.notifications.some(n => /deferred prompts were restored/.test(n.message)));
    await waitFor(() => b.userMessages.length === 1);
    assert.ok(a.notifications.some(n => /handed the turn to/.test(n.message)));
    assert.ok(!a.notifications.some(n => /deferred prompts were restored/.test(n.message)));
  });

  it("a holder expired by a waiter receives a turn-expiry notice without deferred restoration", async () => {
    let clock = Date.now();
    const a = await host({ now: () => clock, queuePollMs: 60_000 }, { mode: "tui" });
    const b = await host({ now: () => clock, queuePollMs: 60_000 });
    await waiting(a, b); a.idle = true; await a.fire("agent_settled");
    clock = store().read().entries[0]!.holdExpiresAt! + 1;
    await sendQueueNudge(socket(b).path, queueKey(repo));
    await waitFor(() => b.userMessages.length === 1);
    await sendQueueNudge(socket(a).path, queueKey(repo));
    await waitFor(() => a.notifications.some(n => /turn expired after 10 idle minutes.*handed over/.test(n.message)));
    assert.equal(a.queueStatuses.at(-1), "queue: on");
    assert.ok(!a.notifications.some(n => /deferred prompts were restored/.test(n.message)));
    assert.deepEqual(await input(a, "next task"), { action: "handled" });
  });

  it("holder input refreshes idle grace under the lock and preserves a running null expiry", async () => {
    let clock = Date.now();
    const a = await host({ now: () => clock, queuePollMs: 60_000 });
    await command(a, "task");
    const previousExpiry = store().read().entries[0]!.holdExpiresAt;
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    let completed = false;
    const gated = input(a, "continue task").then(result => { completed = true; return result; });
    await sleep(40);
    assert.equal(completed, false, "holder input must wait for the file lock");
    assert.equal(store().read().entries[0]!.holdExpiresAt, previousExpiry);
    clock += 10_000;
    rmSync(path);
    assert.deepEqual(await gated, { action: "continue" });
    assert.equal(store().read().entries[0]!.holdExpiresAt, clock + 600_000);
    a.idle = false; await a.fire("agent_start"); clock += 600_000;
    assert.deepEqual(await input(a, "during run"), { action: "continue" });
    assert.equal(store().read().entries[0]!.holdExpiresAt, null);
  });

  it("agent_start drops a lost holder role and warns without releasing the next holder", async () => {
    const a = await host({ queuePollMs: 60_000 }, { mode: "tui" });
    const b = await host({ queuePollMs: 60_000 });
    await command(a, "task"); await command(b, "next task");
    await store().removeIfSameId(store().read().entries[0]!.id);
    a.idle = false; await a.fire("agent_start");
    assert.ok(a.notifications.some(n => /This run does not hold the repository queue turn/.test(n.message)));
    assert.equal(a.queueStatuses.at(-1), "queue: on");
    assert.equal(store().read().entries[0]!.endpointId, socket(b).id);
    assert.match((await a.runTool("queue_done", {})).content[0]!.text, /does not hold/);
    assert.equal(store().read().entries[0]!.endpointId, socket(b).id);
  });

  it("idle grace expiry releases and nudges the next owner", async () => {
    const a = await host({ queueIdleMs: 40 }); const b = await host({ queuePollMs: 60_000 });
    await waiting(a, b); a.idle = true; await a.fire("agent_settled");
    await waitFor(() => b.userMessages.length === 1);
    assert.equal(store().read().entries[0]!.endpointId, socket(b).id);
    assert.ok(a.notifications.some(n => /handed the turn to/.test(n.message)));
  });

  for (const method of ["command", "tool"] as const) {
    it(`${method === "command" ? "/queue done" : "queue_done"} during a run releases at settle, keeps mode on, and the next prompt waits`, async () => {
      const a = await host(); const b = await host(); await waiting(a, b);
      if (method === "command") { await command(a, "done"); assert.match(a.lastNotification!.message, /when this run ends/); }
      else { const result = await a.runTool("queue_done", {}); assert.match(result.content[0]!.text, /when this run ends/); }
      assert.equal(store().read().entries[0]!.endpointId, socket(a).id);
      a.idle = true; await a.fire("agent_settled");
      await waitFor(() => b.userMessages.length === 1);
      assert.deepEqual(await input(a, "another task"), { action: "handled" });
      assert.equal(store().read().entries[1]!.endpointId, socket(a).id);
    });
  }

  it("idle done releases immediately; concurrent duplicate done requests are harmless", async () => {
    const a = await host(); await command(a, "a task");
    await Promise.all([command(a, "done"), a.runTool("queue_done", {})]);
    assert.equal(store().read().entries.length, 0);
    const result = await a.runTool("queue_done", {});
    assert.match(result.content[0]!.text, /does not hold/);
  });

  it("/queue off restores deferred text to the editor, warns about images and stops gating", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    b.editorText = "draft";
    await input(b, "with image", "interactive", [{ type: "image", data: "AA==", mimeType: "image/png" }]);
    await command(b, "OFF");
    assert.equal(b.editorText, "draft\n\nB's task\n\nwith image");
    assert.equal(store().read().entries.length, 1);
    assert.ok(b.notifications.some(n => /images cannot be restored/.test(n.message)));
    assert.equal(await input(b, "not queued"), undefined);
  });

  it("/queue off while running releases at settle without gating later input", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    await command(a, "off");
    assert.equal(store().read().entries.length, 2);
    assert.equal(await input(a, "unguarded"), undefined);
    a.idle = true; await a.fire("agent_settled");
    await waitFor(() => b.userMessages.length === 1);
    assert.equal(store().read().entries.length, 1);
  });

  it("shutdown removes waiting entries, restores text and releases a running holder without post-death calls", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    await b.shutdown("reload");
    assert.equal(b.editorText, "B's task"); assert.equal(store().read().entries.length, 1);
    const c = await host(); await command(c, "C's task");
    await a.shutdown("new"); await waitFor(() => c.userMessages.length === 1);
    assert.equal(store().read().entries[0]!.endpointId, socket(c).id);
  });

  it("shutdown is bounded when another process holds the lock", async () => {
    const a = await host(); await command(a, "task");
    writeFileSync(join(busDir, "queue", `${queueKey(repo)}.lock`), JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    const started = Date.now(); await a.shutdown();
    assert.ok(Date.now() - started < 1000);
    assert.deepEqual(a.callsAfterDeath, []);
  });

  it("peer notes still wake a waiting session; acquisition during that run replays as follow-ups independently of wake policy", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    const target = socket(b);
    const response = await sendNote(target.path, createNote({ from: { id: socket(a).id, sessionId: "a", cwd: repo, replyable: true }, to: target.id, content: "peer heads-up", hops: 1 }));
    assert.equal(response.ok && response.wake, "started");
    assert.equal(b.sent.at(-1)!.options?.["triggerTurn"], true);
    b.idle = false; await b.fire("agent_start");
    await b.runCommand("wake off");
    a.idle = true; await a.fire("agent_settled"); await command(a, "done");
    await waitFor(() => b.userMessages.length === 1);
    assert.equal(b.userMessages[0]!.options?.["deliverAs"], "followUp");
    assert.equal(b.sent.at(-1)!.message.customType, QUEUE_MESSAGE_TYPE);
    assert.equal(b.sent.at(-1)!.options?.["triggerTurn"], false);
    assert.equal(store().read().entries[0]!.holdExpiresAt, null);
  });

  it("non-git cwd, print/json mode and Windows are refused; unavailable queue_done is plain content", async () => {
    const nonGit = await host({ gitToplevel: undefined }); await command(nonGit, "on");
    assert.match(nonGit.lastNotification!.message, /requires a git work tree/);
    assert.equal(await input(nonGit, "regular prompt"), undefined);
    for (const mode of ["print", "json"] as const) {
      const h = await host({}, { mode }); await command(h, "on");
      assert.match(h.lastNotification!.message, /not available.*running bus endpoint/);
      assert.match((await h.runTool("queue_done", {})).content[0]!.text, /not available/);
    }
    const win = await host({ platform: "win32" }); await command(win, "on");
    assert.match(win.lastNotification!.message, /unsupported.*Windows/);
  });

  it("source extension is never gated and queue list does not opt in", async () => {
    const a = await host(); const b = await host();
    await command(b, "list"); assert.match(b.lastNotification!.message, /queue off, none/);
    assert.equal(await input(b, "regular"), undefined);
    await waiting(a, b);
    assert.equal(await input(b, "extension injection", "extension"), undefined);
    assert.equal(store().read().entries.length, 2);
    await command(b, "list"); assert.match(b.lastNotification!.message, /running/);
    assert.match(b.lastNotification!.message, /#2.*B's task.*waited/);
  });

  it("queue_done defaults inactive and only persistent queue mode toggles it, preserving other tool order", async () => {
    const a = await host();
    const state = activeTools.get(a)!;
    assert.equal((a.tool("queue_done") as { defaultActive?: boolean }).defaultActive, false);
    assert.deepEqual(state.names, ["read", "bash", "edit"]);
    await command(a, "list"); await command(a, "");
    assert.deepEqual(state.updates, []);
    await command(a, "on");
    assert.deepEqual(state.names, ["read", "bash", "edit", "queue_done"]);
    await command(a, "on"); await command(a, "list");
    assert.equal(state.updates.length, 1, "unchanged activation skips setActiveTools");
    await command(a, "off"); await command(a, "off");
    assert.deepEqual(state.names, ["read", "bash", "edit"]);
    assert.equal(state.updates.length, 2);
    await command(a, "task");
    assert.deepEqual(state.names, ["read", "bash", "edit", "queue_done"]);
    await command(a, "off");
    assert.deepEqual(state.names, ["read", "bash", "edit"]);
    assert.equal(store().read().entries.length, 0, "off releases immediately while idle");
  });

  it("session_start removes a surviving queue_done activation without reordering other tools", async () => {
    const a = new FakeHost({ cwd: repo });
    const state = stubActiveTools(a, ["edit", "queue_done", "read", "bash"]);
    createSessionBusExtension({ busDir, gitToplevel: () => repo })(a.pi);
    hosts.push(a); await a.start();
    assert.deepEqual(state.names, ["edit", "read", "bash"]);
    assert.deepEqual(state.updates, [["edit", "read", "bash"]]);
  });

  it("only exact single-word subcommands are reserved", async () => {
    const a = await host(); await command(a, "done with the schema");
    assert.equal(a.userMessages[0]!.content, "done with the schema");
  });

  it("a missing waiting entry restores deferred prompts and resets the role", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    await store().removeIfSameId(store().read().entries[1]!.id);
    await waitFor(() => b.editorText === "B's task");
    assert.ok(b.notifications.some(n => /entry disappeared/.test(n.message)));
  });

  it("a missing holder socket is cleaned up and promotes the waiter", async () => {
    const a = await host(); const b = await host({ queuePollMs: 60_000 }); await waiting(a, b);
    rmSync(socket(a).path);
    await sendQueueNudge(socket(b).path, queueKey(repo));
    await waitFor(() => b.userMessages.length === 1);
    assert.equal(store().read().entries.length, 1);
  });

  it("a holder removed while input waits for the lock queues again and announces the expired turn", async () => {
    const a = await host({ queuePollMs: 60_000 }); const b = await host({ queuePollMs: 60_000 }); await waiting(a, b);
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    const gated = input(a, "next");
    await sleep(40);
    const file = store().read(); file.entries.shift();
    file.entries[0]!.state = "active"; file.entries[0]!.holdExpiresAt = null;
    writeFileSync(store().path, JSON.stringify(file), { mode: 0o600 });
    rmSync(path);
    assert.deepEqual(await gated, { action: "handled" });
    assert.equal(store().read().entries[1]!.endpointId, socket(a).id);
    assert.ok(a.notifications.some(n => /turn had expired/.test(n.message)));
  });

  for (const holdingTurn of [false, true]) {
    it(`queue I/O failure fails ${holdingTurn ? "holder refresh" : "new acquisition"} input open with a warning`, async () => {
      const a = await host(); await command(a, holdingTurn ? "task" : "on");
      rmSync(join(busDir, "queue"), { recursive: true });
      symlinkSync(repo, join(busDir, "queue"));
      assert.deepEqual(await input(a, "run despite failure"), { action: "continue" });
      assert.match(a.lastNotification!.message, /I\/O failed; prompts run without the turn/);
    });
  }

  it("an initial /queue prompt also fails open on queue I/O failure", async () => {
    const a = await host(); symlinkSync(repo, join(busDir, "queue"));
    await command(a, "run this task");
    assert.equal(a.userMessages[0]!.content, "run this task");
    assert.ok(a.notifications.some(n => /I\/O failed/.test(n.message)));
  });

  it("acquiring immediately during an existing run sends a separate model-visible notice before the follow-up", async () => {
    const a = await host(); a.idle = false; await a.fire("agent_start");
    await command(a, "queued task during streaming");
    assert.equal(a.sent[0]!.message.customType, QUEUE_MESSAGE_TYPE);
    assert.match(a.sent[0]!.message.content, /holds the repository work-queue turn/);
    assert.equal(a.userMessages[0]!.options?.["deliverAs"], "followUp");
    await waitFor(() => store().read().entries[0]!.holdExpiresAt === null);
  });

  it("a failed agent_start hold write retries within 500 ms before any snapshot check, without waiting for the poll", async () => {
    const a = await host({ queuePollMs: 60_000 }); await command(a, "task");
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    a.idle = false; await a.fire("agent_start");
    assert.notEqual(store().read().entries[0]!.holdExpiresAt, null);
    rmSync(path);
    const opened: string[] = [];
    const snapshotPath = store().path;
    const original = fs.openSync;
    const spy = mock.method(fs, "openSync", (...args: Parameters<typeof fs.openSync>) => {
      if (String(args[0]) === path || String(args[0]) === snapshotPath) opened.push(String(args[0]));
      return original(...args);
    });
    syncBuiltinESMExports();
    const started = Date.now();
    try {
      await waitFor(() => opened.includes(path));
      assert.ok(Date.now() - started < 1200, "retry must be soon, not the 60-second poll");
      assert.equal(opened[0], path, "the retry must acquire the mutation lock before any queue snapshot read");
      assert.equal(store().read().entries[0]!.holdExpiresAt, null);
    } finally { spy.mock.restore(); syncBuiltinESMExports(); }
  });

  it("retries a release requested at settle without adding idle grace", async () => {
    const a = await host(); const b = await host({ queuePollMs: 60_000 }); await waiting(a, b);
    await command(a, "done");
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    a.idle = true; await a.fire("agent_settled");
    assert.equal(store().read().entries[0]!.holdExpiresAt, null, "release failure does not start a grace period");
    rmSync(path);
    await waitFor(() => b.userMessages.length === 1);
  });

  it("off restores text and disables gating even if removal fails, then retries the removal", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "held" }), { mode: 0o600 });
    await command(b, "off");
    assert.equal(b.editorText, "B's task");
    assert.equal(await input(b, "not gated"), undefined);
    rmSync(path);
    await waitFor(() => store().read().entries.length === 1);
    assert.equal(b.userMessages.length, 0);
  });

  it("a resolver completing after shutdown never touches the dead runtime", async () => {
    let resolve!: (repo: string) => void;
    const resolved = new Promise<string>(r => { resolve = r; });
    const a = await host({ gitToplevel: () => resolved });
    const submitted = command(a, "task"); await sleep(10);
    await a.shutdown();
    assert.equal(a.dead, true);
    resolve(repo); await submitted;
    await sleep(30);
    assert.deepEqual(a.userMessages, []);
    assert.deepEqual(a.callsAfterDeath, []);
  });


  it("expiry cleanup rechecks the current snapshot under the lock instead of evicting a newly running holder", async () => {
    const a = await host({ queuePollMs: 60_000 });
    let clock = Date.now();
    const b = await host({ queuePollMs: 60_000, now: () => clock });
    await command(a, "A's task");
    await command(b, "B's task");
    const path = join(busDir, "queue", `${queueKey(repo)}.lock`);
    writeFileSync(path, JSON.stringify({ pid: process.pid, token: "holder-writing" }), { mode: 0o600 });
    clock = store().read().entries[0]!.holdExpiresAt! + 1;
    await sendQueueNudge(socket(b).path, queueKey(repo));
    await sleep(30); // B saw expiry, but cannot normalize while the holder's write is locked.
    const snapshot = store().read(); snapshot.entries[0]!.holdExpiresAt = null;
    writeFileSync(store().path, JSON.stringify(snapshot), { mode: 0o600 });
    rmSync(path);
    await sleep(70);
    assert.equal(b.userMessages.length, 0);
    assert.equal(store().read().entries[0]!.endpointId, socket(a).id);
  });

});
