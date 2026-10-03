/** Opt-in, cooperative repository queue wiring. Shared JSON, never a socket, grants a turn. */
import { execFile } from "node:child_process";
import { realpath } from "node:fs/promises";
import type { ImageContent } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext, InputEvent, InputEventResult } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { createQueueStore, nudgePromoted, probeHolder, type QueueEntry, type QueueStore } from "./core/index.ts";

export const QUEUE_STATUS_KEY = "session-bus-queue";
export const QUEUE_MESSAGE_TYPE = "session-bus.queue-turn";
export async function resolveGitToplevel(cwd: string): Promise<string> {
  const path = await new Promise<string>((resolve, reject) => {
    execFile("git", ["rev-parse", "--show-toplevel"], { cwd, timeout: 5000 }, (error, stdout) => {
      if (error) reject(new Error("Repository work queue requires a git work tree."));
      else resolve(stdout.endsWith("\n") ? stdout.slice(0, -1) : stdout);
    });
  });
  return realpath(path);
}

interface Prompt { text: string; images?: ImageContent[] }
interface Identity { endpointId: string; sessionId: string; name?: string }
export interface QueueWiringOptions {
  busDir(): string;
  context(): ExtensionContext | undefined;
  identity(): Identity | undefined;
  available(): string | undefined;
  resetChain(): void;
  now(): number;
  idleMs?: number;
  pollMs?: number;
  gitToplevel?: (cwd: string) => Promise<string> | string;
}
const flat = (s: string, max = 80): string => s.replace(/[\u0000-\u001f\u007f-\u009f\u2028\u2029]+/g, " ").replace(/"/g, "'").trim().slice(0, max);
const label = (e: QueueEntry): string => e.name ? `"${flat(e.name)}" (id ${e.endpointId})` : `id ${e.endpointId}`;

export function createQueueWiring(pi: ExtensionAPI, options: QueueWiringOptions) {
  const idleMs = options.idleMs ?? 600_000;
  const pollMs = options.pollMs ?? 5000;
  let live = false;
  let closing = false;
  let mode = false;
  let role: "none" | "waiting" | "holding" = "none";
  let store: QueueStore | undefined;
  let own: Identity | undefined;
  let entryId: string | undefined;
  let cancelledId: string | undefined;
  let deferred: Prompt[] = [];
  let followUps: Prompt[] = [];
  let running = false;
  let releaseAtSettle = false;
  let noticePending = false;
  let replayedId: string | undefined;
  let expiryDirty = false;
  let desiredExpiry: number | null = null;
  let idleTimer: NodeJS.Timeout | undefined;
  let pollTimer: NodeJS.Timeout | undefined;
  let nudgeTimer: NodeJS.Timeout | undefined;
  let holdRetryTimer: NodeJS.Timeout | undefined;
  let checking: Promise<void> | undefined;
  let operations: Promise<unknown> = Promise.resolve();
  let generation = 0;

  function serial<T>(fn: () => Promise<T>): Promise<T> {
    const result = operations.then(fn);
    operations = result.catch(() => undefined);
    return result;
  }
  function context() { return live ? options.context() : undefined; }
  function notify(text: string, level: "info" | "warning" = "info") { context()?.ui.notify(text, level); }
  function warning(error: unknown) { notify(`Work queue I/O failed; prompts run without the turn: ${flat(String(error), 200)}`, "warning"); }
  function activateDone(active: boolean) {
    try {
      if (typeof pi.getActiveTools !== "function" || typeof pi.setActiveTools !== "function") return;
      const tools = pi.getActiveTools();
      if (tools.includes("queue_done") === active) return;
      pi.setActiveTools(active ? [...tools, "queue_done"] : tools.filter(name => name !== "queue_done"));
    } catch (error) { notify(`Could not update queue_done activation: ${flat(String(error), 200)}`, "warning"); }
  }
  function lostTurn() {
    role = "none"; entryId = undefined; noticePending = releaseAtSettle = expiryDirty = false;
    clearTimeout(idleTimer); clearTimeout(holdRetryTimer);
    idleTimer = holdRetryTimer = undefined;
    notify(running
      ? "This run does not hold the repository queue turn; your next prompt will queue again."
      : `Your repository queue turn expired after ${idleMs / 60_000} idle minutes and was handed over; your next prompt will queue again.`, "warning");
    status();
  }
  async function writeHold() {
    const id = entryId!;
    const result = await store!.setHoldExpiry(id, desiredExpiry);
    expiryDirty = false;
    clearTimeout(holdRetryTimer); holdRetryTimer = undefined;
    if (!result.file.entries.some(e => e.id === id && e.state === "active")) lostTurn();
    await nudge(result.promoted);
  }
  function retryHoldSoon() {
    if (holdRetryTimer || !live || closing) return;
    // One bounded retry per failed agent_start write; further failures use the normal poll.
    holdRetryTimer = setTimeout(() => { holdRetryTimer = undefined; void check(); }, 500);
    holdRetryTimer.unref();
  }
  function status() {
    const c = context();
    if (!c || c.mode !== "tui") return;
    let text: string | undefined;
    if (mode) {
      text = role === "holding" ? `queue: turn${running ? "" : " (idle)"}` : "queue: on";
      if (role === "waiting") {
        try { text = `queue: #${store!.read().entries.findIndex(e => e.id === entryId) + 1}`; } catch { text = "queue: waiting"; }
      }
    }
    c.ui.setStatus(QUEUE_STATUS_KEY, text);
  }
  function clearTimers() {
    clearTimeout(idleTimer); clearTimeout(pollTimer); clearTimeout(nudgeTimer); clearTimeout(holdRetryTimer);
    idleTimer = pollTimer = nudgeTimer = holdRetryTimer = undefined;
  }
  function restore() {
    const prompts = [...deferred, ...followUps];
    deferred = []; followUps = [];
    const c = context();
    if (!c || !prompts.length) return;
    const text = prompts.map(p => p.text).join("\n\n");
    try { c.ui.setEditorText([c.ui.getEditorText(), text].filter(Boolean).join("\n\n")); }
    catch { notify(`Could not restore deferred prompts to the editor:\n${text}`, "warning"); }
    if (prompts.some(p => p.images?.length)) notify("Deferred text restored; attached images cannot be restored to the editor.", "warning");
  }
  function poll() {
    clearTimeout(pollTimer);
    if (!live || closing || role === "none" && !cancelledId) return;
    pollTimer = setTimeout(() => { pollTimer = undefined; void check().finally(poll); }, pollMs);
    pollTimer.unref();
  }
  function armIdle(expiry: number) {
    clearTimeout(idleTimer);
    if (!live || closing) return;
    idleTimer = setTimeout(() => {
      idleTimer = undefined;
      void serial(async () => {
        if (!live || closing || role !== "holding" || running) return;
        if (options.now() < expiry) { armIdle(expiry); return; }
        try { await release(); } catch (error) { warning(error); armIdle(options.now() + Math.min(pollMs, 1000)); }
      });
    }, Math.max(1, expiry - options.now()));
    idleTimer.unref();
  }
  function turnMessage() {
    const count = store!.read().entries.filter(e => e.state === "waiting").length;
    return {
      customType: QUEUE_MESSAGE_TYPE,
      content: `This session holds the repository work-queue turn for ${flat(store!.repo, 500)}. ${count} session(s) are waiting. Call queue_done once the user's task is completely finished and you are not asking or waiting for the user's reply. It only hands over the repository turn.`,
      display: true,
      details: { repo: store!.repo, waiting: count },
    };
  }
  function send(prompt: Prompt, followUp: boolean) {
    const content = prompt.images?.length ? [{ type: "text" as const, text: prompt.text }, ...prompt.images] : prompt.text;
    pi.sendUserMessage(content, { expandPromptTemplates: true, ...(followUp ? { deliverAs: "followUp" as const } : {}) });
  }
  function acquire(entry: QueueEntry, replay: boolean) {
    if (!live || closing) return;
    role = "holding"; entryId = entry.id;
    options.resetChain(); noticePending = true;
    const busy = running || !context()!.isIdle();
    if (busy) {
      clearTimeout(idleTimer); expiryDirty = true; desiredExpiry = null;
      pi.sendMessage(turnMessage(), { triggerTurn: false, deliverAs: "steer" });
      noticePending = false;
    } else if (entry.holdExpiresAt !== null) armIdle(entry.holdExpiresAt);
    status(); poll();
    if (!replay || replayedId === entry.id) return;
    replayedId = entry.id;
    const prompts = deferred.splice(0);
    if (!prompts.length) return;
    if (busy) {
      for (const prompt of prompts) send(prompt, true);
    } else {
      // sendUserMessage is fire-and-forget. Wait for agent_start before enqueueing the rest,
      // otherwise asynchronous input/template expansion can race the first prompt's startup.
      followUps.push(...prompts.slice(1));
      send(prompts[0]!, false);
    }
  }
  async function nudge(entry: QueueEntry | undefined) {
    try {
      return await nudgePromoted(store!, entry, {
        ownEndpointId: own?.endpointId,
        onOwn: e => acquire(e, true),
        timeoutMs: 150,
        maxAttempts: 3,
      });
    } catch (error) {
      // A nudge is best effort. In particular it must not fail open a prompt already deferred.
      notify(`Work queue nudge failed: ${flat(String(error), 200)}`, "warning");
      return entry;
    }
  }
  async function removeCancelled() {
    if (!cancelledId) return;
    const result = await store!.removeIfSameId(cancelledId);
    cancelledId = undefined;
    await nudge(result.promoted);
  }
  async function enable(): Promise<boolean> {
    const reason = options.available();
    if (reason) { notify(`Work queue not available: ${reason}`, "warning"); return false; }
    if (mode) return true;
    if (!store) {
      const version = generation;
      try {
        const repo = await (options.gitToplevel ?? resolveGitToplevel)(context()!.cwd);
        if (!live || closing || version !== generation) return false;
        own = options.identity();
        if (!own) { notify("Work queue not available: no running bus endpoint.", "warning"); return false; }
        store = createQueueStore({ busDir: options.busDir(), repo, idleMs, now: options.now });
        try { store.read(); } catch (error) { warning(error); }
      } catch (error) { if (live && !closing) notify(error instanceof Error ? error.message : String(error), "warning"); return false; }
    }
    mode = true; status(); return true;
  }
  async function gate(prompt: Prompt): Promise<InputEventResult> {
    if (!live || closing || !mode) return { action: "continue" };
    try {
      await removeCancelled();
      if (role === "holding") {
        let confirmed = false;
        let expiry: number | null = null;
        const result = await store!.mutate((file, now) => {
          const entry = file.entries.find(e => e.id === entryId && e.state === "active");
          if (!entry) return;
          confirmed = true;
          if (entry.holdExpiresAt !== null) entry.holdExpiresAt = now + idleMs;
          expiry = entry.holdExpiresAt;
        });
        if (confirmed) {
          if (!running && expiry !== null) {
            desiredExpiry = expiry; expiryDirty = false; armIdle(expiry);
          }
          return { action: "continue" };
        }
        role = "none"; entryId = undefined; noticePending = releaseAtSettle = expiryDirty = false;
        clearTimeout(idleTimer); clearTimeout(holdRetryTimer);
        notify("Your repository queue turn had expired; this prompt will queue again.", "warning");
        await nudge(result.promoted);
      }
      const file = store!.read();
      const entry = file.entries.find(e => e.id === entryId);
      if (role === "waiting" && entry?.state === "active") {
        deferred.push(prompt); acquire(entry, true); return { action: "handled" };
      }
      if (role === "waiting" && !entry) { role = "none"; entryId = undefined; restore(); }
      const result = await store!.enqueue({ ...own!, pid: process.pid, title: prompt.text });
      if (!live || closing) return { action: "handled" };
      entryId = result.entry.id;
      if (result.entry.state === "active") {
        acquire(result.entry, false);
        return { action: "continue" };
      }
      role = "waiting"; deferred.push(prompt); status(); poll();
      notify(`queued #${result.position} behind ${result.holder ? label(result.holder) : "the holder"}`);
      if (result.promoted) await nudge(result.promoted);
      return { action: "handled" };
    } catch (error) {
      warning(error);
      // A failed new acquisition has not consumed this prompt. Do not retain it for replay.
      return { action: "continue" };
    }
  }
  async function release(): Promise<string> {
    if (role !== "holding" || !entryId) return "This session does not hold the repository turn.";
    const id = entryId;
    const result = await store!.removeIfSameId(id);
    role = "none"; entryId = undefined; releaseAtSettle = false; noticePending = false; expiryDirty = false;
    clearTimeout(idleTimer); clearTimeout(pollTimer); clearTimeout(holdRetryTimer);
    holdRetryTimer = undefined;
    const next = result.promoted ? await nudge(result.promoted) : result.file.entries[0];
    const text = next ? `handed the turn to ${label(next)}` : "released; nobody waiting";
    notify(text); status();
    return text;
  }
  async function done(): Promise<string> {
    const reason = options.available();
    if (reason || !live || closing) return `Work queue not available: ${reason ?? "session is shutting down"}`;
    if (role !== "holding") return "This session does not hold the repository turn.";
    if (running || !context()!.isIdle()) {
      releaseAtSettle = true;
      return "The repository turn will be handed over when this run ends.";
    }
    releaseAtSettle = true;
    return release();
  }
  async function checkBody() {
    if (!live || closing || !store || role === "none" && !cancelledId) return;
    try {
      // A pending running/idle hold write must precede reads and cooperative cleanup.
      if (role === "holding" && expiryDirty) await writeHold();
      await removeCancelled();
      if (role === "none") { status(); return; }
      if (role === "holding" && releaseAtSettle && !running) { await release(); return; }
      let file = store.read();
      const holder = file.entries[0];
      const expired = holder && holder.holdExpiresAt !== null && holder.holdExpiresAt <= options.now();
      const dead = holder && !expired && role === "waiting" && await probeHolder(options.busDir(), holder, { timeoutMs: 150 }) === "dead";
      if (role === "waiting" && (expired || dead)) {
        // Expiry is rechecked by normalization under the lock: an old snapshot must not
        // evict a holder that has since started running or refreshed its grace period.
        const result = expired ? await store.mutate(() => {}) : await store.removeIfSameId(holder!.id);
        await nudge(result.promoted);
        file = store.read();
      } else if (role === "waiting" && (!holder || holder.state !== "active")) {
        const result = await store.mutate(() => {});
        await nudge(result.promoted); file = result.file;
      }
      if (!live || closing) return;
      const entry = file.entries.find(e => e.id === entryId);
      if (role === "holding" && entry?.state !== "active") {
        lostTurn();
      } else if (!entry) {
        role = "none"; entryId = undefined; noticePending = false; expiryDirty = false;
        clearTimeout(idleTimer); restore();
        notify("Your repository queue entry disappeared; deferred prompts were restored to the editor.", "warning");
      } else if (entry.state === "active" && role === "waiting") acquire(entry, true);
      // A waiter promoted during this check may already be running.
      if (role === "holding" && expiryDirty) await writeHold();
      status();
    } catch (error) { if (live && !closing) warning(error); }
  }
  function check(): Promise<void> {
    if (!checking) {
      checking = serial(checkBody).finally(() => { checking = undefined; });
    }
    return checking;
  }

  pi.on("before_agent_start", () => {
    if (!live || closing || role !== "holding" || !noticePending) return;
    try { const message = turnMessage(); noticePending = false; return { message }; }
    catch (error) { warning(error); }
  });
  pi.on("agent_start", async () => {
    if (!live || closing) return;
    running = true;
    await serial(async () => {
      if (!live || closing) return;
      if (role === "holding") {
        clearTimeout(idleTimer); desiredExpiry = null; expiryDirty = true;
        try { await writeHold(); }
        catch (error) { warning(error); retryHoldSoon(); poll(); }
      }
      for (const prompt of followUps.splice(0)) send(prompt, true);
      status();
    });
  });
  pi.on("agent_settled", async () => {
    if (!live || closing) return;
    running = false;
    clearTimeout(holdRetryTimer); holdRetryTimer = undefined;
    await serial(async () => {
      if (!live || closing || role !== "holding") return;
      try {
        if (releaseAtSettle) { await release(); return; }
        desiredExpiry = options.now() + idleMs; expiryDirty = true;
        armIdle(desiredExpiry);
        await writeHold();
      } catch (error) { warning(error); poll(); }
      status();
    });
  });
  pi.registerTool({
    name: "queue_done", label: "Queue done",
    defaultActive: false,
    description: "Hand over this session's repository work-queue turn when the user's task is completely finished. If a run is in progress, release when it settles. Does not end the conversation or change queue mode.",
    promptSnippet: "Hand over the repository work-queue turn after completely finishing the user's queued task",
    promptGuidelines: ["Call queue_done only when the user's queued task is completely finished and you are not asking or waiting for the user's reply. It only hands over the repository turn."],
    parameters: Type.Object({}),
    async execute() {
      let text: string;
      try { text = await serial(done); } catch (error) { text = `Could not release repository turn: ${flat(String(error), 200)}`; }
      return { content: [{ type: "text", text }], details: {} };
    },
  });
  pi.registerCommand("queue", {
    description: "Repository work queue: /queue [list|on|off|done|<prompt>]",
    async handler(args) {
      await serial(async () => {
        if (!live || closing) return;
        const text = args.trim(); const cmd = text.toLowerCase();
        const reason = options.available();
        if (reason) { notify(`Work queue not available: ${reason}`, "warning"); return; }
        try {
          if (cmd === "done") { notify(await done()); return; }
          if (cmd === "off") {
            mode = false; activateDone(false); status();
            try {
              if (role === "waiting") {
                cancelledId = entryId;
                role = "none"; entryId = undefined; restore();
                await removeCancelled();
              } else if (role === "holding") notify(await done());
            } finally { poll(); status(); }
            notify("Repository queue mode is off."); return;
          }
          if (cmd === "on") {
            if (await enable()) { activateDone(true); notify("Repository queue mode is on."); } return;
          }
          if (!text || cmd === "list") {
            // Listing may resolve the repository but must not opt the session in.
            const previous = mode;
            if (!await enable()) return;
            mode = previous; status();
            const file = store!.read(); const holder = file.entries[0];
            const lines = [`Repository: ${store!.repo}`, `Holder: ${holder ? `${label(holder)} · ${holder.holdExpiresAt === null ? "running" : `idle (${Math.max(0, Math.ceil((holder.holdExpiresAt - options.now()) / 1000))}s grace remaining)`}` : "nobody"}`];
            for (const [i, e] of file.entries.slice(1).entries()) lines.push(`#${i + 2} ${label(e)} · ${flat(e.title)} · waited ${Math.max(0, Math.floor((options.now() - Date.parse(e.enqueuedAt)) / 1000))}s`);
            lines.push(`This session: queue ${mode ? "on" : "off"}, ${role}.`);
            notify(lines.join("\n")); return;
          }
          if (!await enable()) return;
          activateDone(true);
          options.resetChain();
          const result = await gate({ text });
          if (live && !closing && result.action === "continue") send({ text }, running || !context()!.isIdle());
        } catch (error) { warning(error); }
      });
    },
  });

  return {
    start() {
      generation++; live = true; closing = false; mode = false; role = "none";
      store = undefined; own = undefined; entryId = undefined; deferred = []; followUps = [];
      cancelledId = undefined;
      running = false; releaseAtSettle = noticePending = expiryDirty = false; replayedId = undefined;
      clearTimers(); activateDone(false); status();
    },
    input(event: InputEvent): Promise<InputEventResult> | undefined {
      if (!live || closing || !mode || event.source === "extension") return;
      return serial(() => gate({ text: event.text, ...(event.images ? { images: event.images } : {}) }));
    },
    onNudge(key: string) {
      if (!live || closing || key !== store?.key || nudgeTimer) return;
      nudgeTimer = setTimeout(() => { nudgeTimer = undefined; void check(); }, 0);
      nudgeTimer.unref();
    },
    async shutdown() {
      if (!live || closing) return;
      closing = true; generation++; clearTimers(); restore();
      mode = false; status();
      const oldOwn = own;
      // Use the same snapshot with a shorter lock deadline specifically for bounded shutdown.
      const cleanupStore = store && createQueueStore({ busDir: options.busDir(), repo: store.repo, idleMs, now: options.now, lockTimeoutMs: 250 });
      const version = generation;
      let timeout: NodeJS.Timeout | undefined;
      const cleanup = serial(async () => {
        if (!cleanupStore || !oldOwn) return;
        try {
          const result = await cleanupStore.removeEntry(oldOwn.endpointId, process.pid);
          const next = await nudgePromoted(cleanupStore, result.promoted, { timeoutMs: 100, maxAttempts: 2 });
          if (live && generation === version) notify(next ? `handed the turn to ${label(next)}` : "released; nobody waiting");
        } catch (error) { if (live && generation === version) notify(`Work queue shutdown cleanup failed: ${flat(String(error), 200)}`, "warning"); }
      });
      await Promise.race([cleanup, new Promise<void>(resolve => { timeout = setTimeout(resolve, 900); })]);
      clearTimeout(timeout); live = false; role = "none"; entryId = undefined;
    },
  };
}
