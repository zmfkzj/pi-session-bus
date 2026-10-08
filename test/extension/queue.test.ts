import assert from "node:assert/strict";
import { mkdirSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import fs from "node:fs";
import { syncBuiltinESMExports } from "node:module";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it, mock } from "node:test";
import { createSessionBusExtension, type SessionBusOptions } from "../../src/index.ts";
import { QUEUE_MESSAGE_TYPE } from "../../src/queue.ts";
import { createNote, createQueueStore, deriveId, listSockets, queueKey, sendNote, sendQueueNudge } from "../../src/core/index.ts";
import { holdQueueLock, makeTempDir, removeTempDir } from "../core/helpers.ts";
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
  createSessionBusExtension({ busDir, gitToplevel: async () => repo, queuePollMs: 20, queueImageDir: busDir, ...opts })(h.pi);
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
    const held = holdQueueLock(busDir, repo);
    let completed = false;
    const gated = input(a, "continue task").then(result => { completed = true; return result; });
    await sleep(40);
    assert.equal(completed, false, "holder input must wait for the file lock");
    assert.equal(store().read().entries[0]!.holdExpiresAt, previousExpiry);
    clock += 10_000;
    held.release();
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

  it("/queue off restores deferred prompts to the editor, with their images saved as clipboard files, and stops gating", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    b.editorText = "draft";
    await input(b, "with image", "interactive", [{ type: "image", data: "AA==", mimeType: "image/png" }, { type: "image", data: "AQ==", mimeType: "image/bmp" }]);
    await command(b, "OFF");
    const match = /^draft\n\nB's task\n\nwith image\n(\S+\/pi-clipboard-[0-9a-f-]{36}\.png)$/.exec(b.editorText);
    assert.ok(match, b.editorText);
    assert.ok(match[1]!.startsWith(busDir));
    assert.equal(fs.readFileSync(match[1]!).toString("base64"), "AA==");
    assert.equal(fs.statSync(match[1]!).mode & 0o777, 0o600);
    assert.equal(store().read().entries.length, 1);
    assert.ok(b.notifications.some(n => /1 image\(s\) were saved as clipboard files/.test(n.message)));
    assert.ok(b.notifications.some(n => /1 image\(s\) of the deferred prompts could not be saved/.test(n.message)), "an image type pi-images does not read");
    assert.equal(await input(b, "not queued"), undefined);
  });

  it("keeps pasted images of held-back prompts through replay exactly once, and attaches them to a prompt that runs now", async () => {
    const a = await host(); const b = await host();
    const clip = "/tmp/pi-clipboard-0f0f0f0f-0000-4000-8000-000000000000.png";
    const pasted = { type: "image", data: "UE5H", mimeType: "image/png" };
    const rpc = { type: "image", data: "QUJD", mimeType: "image/png" };
    const asked: { text: string; existing: readonly unknown[] }[] = [];
    // The images extension provides a pasted image once per prompt that mentions it, never one the prompt carries.
    b.imageProvider = (request) => {
      asked.push(request);
      return request.text.includes(clip) && !request.existing.some(image => (image as { data: string }).data === pasted.data) ? [pasted] : [];
    };
    await holding(a); await command(b, "on");
    assert.deepEqual(await input(b, `${clip} describe this`, "rpc", [rpc]), { action: "handled" });
    assert.deepEqual(await input(b, `then compare ${clip}`), { action: "handled" });
    assert.deepEqual(asked.map(request => request.existing.length), [1, 0], "asked on submission, with the images the prompt carries");
    assert.equal(await input(b, `${clip} from an extension`, "extension"), undefined, "extension input is never given images");
    assert.equal(asked.length, 2);
    await command(a, "done"); a.idle = true; await a.fire("agent_settled");
    await waitFor(() => b.userMessages.length === 1);
    assert.deepEqual(b.userMessages[0]!.content, [{ type: "text", text: `${clip} describe this` }, rpc, pasted]);
    b.idle = false; await b.fire("agent_start");
    await waitFor(() => b.userMessages.length === 2);
    assert.deepEqual(b.userMessages[1], { content: [{ type: "text", text: `then compare ${clip}` }, pasted], options: { expandPromptTemplates: true, deliverAs: "followUp" } });
    // The holder's prompt runs now and carries its pasted image; the images extension adds none twice.
    assert.deepEqual(await input(b, `and ${clip}`), { action: "transform", text: `and ${clip}`, images: [pasted] });
    assert.deepEqual(await input(b, "no image"), { action: "continue" });
  });

  it("/queue <prompt> attaches its pasted images whether it runs now or waits", async () => {
    const a = await host(); const b = await host();
    const pasted = { type: "image", data: "UE5H", mimeType: "image/png" };
    a.imageProvider = b.imageProvider = (request) => request.text.includes("shot.png") ? [pasted] : [];
    await command(a, "look at shot.png");
    assert.deepEqual(a.userMessages[0]!.content, [{ type: "text", text: "look at shot.png" }, pasted]);
    a.idle = false; await a.fire("agent_start");
    await command(b, "and shot.png");
    assert.equal(b.userMessages.length, 0);
    await command(a, "done"); a.idle = true; await a.fire("agent_settled");
    await waitFor(() => b.userMessages.length === 1);
    assert.deepEqual(b.userMessages[0]!.content, [{ type: "text", text: "and shot.png" }, pasted]);
    await command(b, "list");
    assert.equal(b.events.filter(event => event.channel === "pi-images:attachments").length, 1, "reserved words are not prompts");
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
    holdQueueLock(busDir, repo);
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
    const held = holdQueueLock(busDir, repo);
    const gated = input(a, "next");
    await sleep(40);
    const file = store().read(); file.entries.shift();
    file.entries[0]!.state = "active"; file.entries[0]!.holdExpiresAt = null;
    held.publish(file);
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

  /** An older session-bus (the test runner's pid) holding the v1 turn; returns the file's path and content. */
  function legacyHolder(): { path: string; v1: string } {
    mkdirSync(join(busDir, "queue"), { recursive: true, mode: 0o700 });
    const path = join(busDir, "queue", `${queueKey(repo)}.json`);
    const v1 = JSON.stringify({ v: 1, repo, entries: [{ id: crypto.randomUUID(), endpointId: "0000abcd", pid: process.ppid, sessionId: "old",
      title: "old task", state: "active", enqueuedAt: new Date().toISOString(), grantedAt: new Date().toISOString(), holdExpiresAt: null }] });
    writeFileSync(path, v1, { mode: 0o600 });
    return { path, v1 };
  }

  it("holds prompts while an older session holds the v1 turn, keeps their images, and runs them in order once it is gone", async () => {
    const a = await host({}, { mode: "tui" });
    const pasted = { type: "image", data: "UE5H", mimeType: "image/png" };
    a.imageProvider = (request) => request.text.includes("shot.png") ? [pasted] : [];
    const legacy = legacyHolder();
    await command(a, "run this task with shot.png");
    assert.deepEqual(await input(a, "and then this"), { action: "handled" });
    await new Promise(resolve => setTimeout(resolve, 80)); // the poll keeps holding
    assert.equal(a.userMessages.length, 0, "nothing runs alongside the older session's turn");
    assert.ok(a.notifications.some(n => new RegExp(`older session-bus \\(pid ${process.ppid}\\).*held here, not run\\. In that session run /queue off \\(or finish its turn\\), then reload it\\. Keep this session running`).test(n.message)), JSON.stringify(a.notifications));
    assert.ok(a.notifications.some(n => /^held: an older session-bus/.test(n.message)));
    assert.equal(a.queueStatuses.at(-1), "queue: held (older session-bus)");
    assert.equal(store().read().entries.length, 0, "no v2 turn was granted");
    assert.equal(fs.readFileSync(legacy.path, "utf8"), legacy.v1, "the v1 file is left unchanged");
    // The supported procedure: in the older session /queue off removes its v1 entry, then it reloads. Held prompts
    // then run here in order, the first with its image, without this session being reloaded.
    writeFileSync(legacy.path, JSON.stringify({ v: 1, repo, entries: [] }), { mode: 0o600 });
    await waitFor(() => a.userMessages.length === 1);
    assert.deepEqual(a.userMessages[0]!.content, [{ type: "text", text: "run this task with shot.png" }, pasted]);
    a.idle = false; await a.fire("agent_start");
    await waitFor(() => a.userMessages.length === 2);
    assert.deepEqual(a.userMessages[1], { content: "and then this", options: { expandPromptTemplates: true, deliverAs: "followUp" } });
    assert.equal(store().read().entries[0]!.state, "active");
    assert.equal(a.queueStatuses.at(-1), "queue: turn");
  });

  it("a reload while prompts are held returns them in order with their images saved, and submitting them again attaches each image once", async () => {
    const a = await host();
    const pasted = { type: "image", data: "UE5H", mimeType: "image/png" };
    a.imageProvider = (request) => request.text.includes("shot.png") ? [pasted] : [];
    const legacy = legacyHolder();
    await command(a, "first with shot.png");
    await input(a, "second", "rpc", [{ type: "image", data: "QUJD", mimeType: "image/png" }]);
    await input(a, "third");
    await a.shutdown("reload");
    const prompts = a.editorText.split("\n\n");
    assert.equal(prompts.length, 3, a.editorText);
    assert.match(prompts[0]!, /^first with shot\.png\n\S+\/pi-clipboard-[0-9a-f-]{36}\.png$/);
    assert.match(prompts[1]!, /^second\n\S+\/pi-clipboard-[0-9a-f-]{36}\.png$/);
    assert.equal(prompts[2], "third");
    assert.equal(fs.readFileSync(legacy.path, "utf8"), legacy.v1);
    // After the reload the older session is gone and the prompts are submitted again, in order. pi-images attaches
    // clipboard files named in a prompt (here: a provider that reads them), each image once.
    writeFileSync(legacy.path, JSON.stringify({ v: 1, repo, entries: [] }), { mode: 0o600 });
    const b = await host();
    b.imageProvider = (request) => [...request.text.matchAll(/\S+\/pi-clipboard-[0-9a-f-]{36}\.png/g)]
      .map(([path]) => ({ type: "image", data: fs.readFileSync(path).toString("base64"), mimeType: "image/png" }))
      .filter(image => !request.existing.some(other => (other as { data: string }).data === image.data));
    await command(b, "on");
    const results = [];
    for (const prompt of prompts) results.push(await input(b, prompt));
    assert.deepEqual(results, [
      { action: "transform", text: prompts[0], images: [pasted] },
      { action: "transform", text: prompts[1], images: [{ type: "image", data: "QUJD", mimeType: "image/png" }] },
      { action: "continue" },
    ]);
  });

  it("a holder whose v1 peer takes a turn holds new prompts and replays them once the peer is gone", async () => {
    const a = await host();
    await command(a, "first task");
    assert.equal(a.userMessages.length, 1);
    const legacy = legacyHolder();
    assert.deepEqual(await input(a, "second task"), { action: "handled" });
    assert.equal(a.userMessages.length, 1);
    writeFileSync(legacy.path, JSON.stringify({ v: 1, repo, entries: [] }), { mode: 0o600 });
    await waitFor(() => a.userMessages.length === 2);
    assert.equal(a.userMessages[1]!.content, "second task");
  });

  it("/queue off while held returns held prompts to the editor and runs nothing", async () => {
    const a = await host();
    const legacy = legacyHolder();
    await command(a, "first"); await input(a, "second");
    await command(a, "off");
    assert.equal(a.editorText, "first\n\nsecond");
    await new Promise(resolve => setTimeout(resolve, 60));
    assert.equal(a.userMessages.length, 0);
    assert.equal(fs.readFileSync(legacy.path, "utf8"), legacy.v1);
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
    const held = holdQueueLock(busDir, repo);
    a.idle = false; await a.fire("agent_start");
    assert.notEqual(store().read().entries[0]!.holdExpiresAt, null);
    held.release();
    const opened: string[] = [];
    const original = fs.openSync;
    const spy = mock.method(fs, "openSync", (...args: Parameters<typeof fs.openSync>) => {
      const path = String(args[0]);
      if (/\.l\.tmp$/.test(path)) opened.push("lock");
      else if (/\/S\d{15}\.json$/.test(path)) opened.push("snapshot");
      return original(...args);
    });
    syncBuiltinESMExports();
    const started = Date.now();
    try {
      await waitFor(() => opened.includes("lock"));
      assert.ok(Date.now() - started < 1200, "retry must be soon, not the 60-second poll");
      assert.equal(opened[0], "lock", "the retry must acquire the mutation lock before any queue snapshot read");
      assert.equal(store().read().entries[0]!.holdExpiresAt, null);
    } finally { spy.mock.restore(); syncBuiltinESMExports(); }
  });

  it("retries a release requested at settle without adding idle grace", async () => {
    const a = await host(); const b = await host({ queuePollMs: 60_000 }); await waiting(a, b);
    await command(a, "done");
    const held = holdQueueLock(busDir, repo);
    a.idle = true; await a.fire("agent_settled");
    assert.equal(store().read().entries[0]!.holdExpiresAt, null, "release failure does not start a grace period");
    held.release();
    await waitFor(() => b.userMessages.length === 1);
  });

  it("off restores text and disables gating even if removal fails, then retries the removal", async () => {
    const a = await host(); const b = await host(); await waiting(a, b);
    const held = holdQueueLock(busDir, repo);
    await command(b, "off");
    assert.equal(b.editorText, "B's task");
    assert.equal(await input(b, "not gated"), undefined);
    held.release();
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
    const held = holdQueueLock(busDir, repo); // the holder is writing
    clock = store().read().entries[0]!.holdExpiresAt! + 1;
    await sendQueueNudge(socket(b).path, queueKey(repo));
    await sleep(30); // B saw expiry, but cannot normalize while the holder's write is locked.
    const snapshot = store().read(); snapshot.entries[0]!.holdExpiresAt = null;
    held.publish(snapshot);
    await sleep(70);
    assert.equal(b.userMessages.length, 0);
    assert.equal(store().read().entries[0]!.endpointId, socket(a).id);
  });

});
