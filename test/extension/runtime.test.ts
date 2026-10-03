/**
 * Real-runtime test: two real Pi AgentSessions in one process share a temp bus dir.
 * No network: a faux provider (via ModelRuntime + InMemoryCredentialStore) scripts the models.
 * Nothing touches the real ~/.pi/agent (temp agentDir, temp bus dir, in-memory settings/sessions).
 */

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
  InMemoryCredentialStore,
  type AssistantMessage,
  type TranscriptContext,
} from "@earendil-works/pi-ai";
import {
  createAgentSession,
  defineTool,
  DefaultResourceLoader,
  ModelRuntime,
  SessionManager,
  SettingsManager,
  type AgentSession,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { createQueueStore, deriveId, listSockets, socketFileName } from "../../src/core/index.ts";
import { createSessionBusExtension, type SessionBusOptions } from "../../src/index.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";

type Step = (context: TranscriptContext) => AssistantMessage | Promise<AssistantMessage>;

interface Node {
  name: string;
  session: AgentSession;
  /** One entry per model request: the whole request (messages) serialized at request time. */
  requests: string[];
  /** Number of agent runs (agent_start events). */
  runs: number;
  script(steps: Step[]): void;
  /** Bus id of this node's endpoint. */
  id(): string;
}

const say = (text: string): AssistantMessage => fauxAssistantMessage(text);
const call = (name: string, args: Parameters<typeof fauxToolCall>[1]): AssistantMessage =>
  fauxAssistantMessage([fauxToolCall(name, args)], { stopReason: "toolUse" });

function deferred<T = void>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

async function waitFor(condition: () => boolean, what: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

let tmp: string;
let busDir: string;
let nodes: Node[];
let serial = 0;

beforeEach(() => {
  tmp = makeTempDir("sb-rt");
  busDir = join(tmp, "bus");
  nodes = [];
});

afterEach(async () => {
  for (const node of nodes) {
    // session.dispose() does not emit session_shutdown (the Pi modes do); do what a mode does on quit.
    await node.session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    node.session.dispose();
  }
  assert.deepEqual(listSockets(busDir), [], "every endpoint must remove its socket on shutdown");
  assert.deepEqual(existsSync(busDir) ? readdirSync(busDir).filter(name => name !== "queue") : [], [], "no sockets or unrelated files remain");
  const sharedRepo = join(tmp, "shared-repo");
  if (existsSync(join(busDir, "queue"))) {
    assert.equal(createQueueStore({ busDir, repo: sharedRepo }).read().entries.length, 0, "shutdown removes every queue entry");
    assert.ok(readdirSync(join(busDir, "queue")).every(name => name.endsWith(".json")), "no locks or temporary files remain");
  }
  removeTempDir(tmp);
});

async function makeNode(
  name: string,
  extensionOptions: SessionBusOptions = {},
  customTools: ReturnType<typeof defineTool>[] = [],
  cwdOverride?: string,
): Promise<Node> {
  const cwd = cwdOverride ?? join(tmp, `cwd-${name}`);
  const agentDir = join(tmp, `agent-${name}`);
  mkdirSync(cwd, { recursive: true });
  mkdirSync(agentDir, { recursive: true });

  const faux = fauxProvider({ provider: `bus-faux-${++serial}` });
  const runtime = await ModelRuntime.create({
    credentials: new InMemoryCredentialStore(),
    modelsPath: null,
    refreshOnCreate: false,
  });
  runtime.registerNativeProvider(faux.provider);
  const model = runtime.getModel(faux.provider.id, faux.getModel().id);
  assert.ok(model, "faux model is registered");

  const settingsManager = SettingsManager.inMemory({
    compaction: { enabled: false },
    retry: { enabled: false },
  });
  const resourceLoader = new DefaultResourceLoader({
    cwd,
    agentDir,
    settingsManager,
    extensionFactories: [createSessionBusExtension({ busDir, ...extensionOptions })],
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
  });
  await resourceLoader.reload();
  const { session } = await createAgentSession({
    cwd,
    agentDir,
    modelRuntime: runtime,
    model,
    thinkingLevel: "off",
    noTools: "builtin",
    customTools,
    resourceLoader,
    sessionManager: SessionManager.inMemory(cwd),
    settingsManager,
  });

  const node: Node = {
    name,
    session,
    requests: [],
    runs: 0,
    script(steps) {
      faux.setResponses(
        steps.map((step): ((context: TranscriptContext) => Promise<AssistantMessage>) => async (context) => {
          node.requests.push(JSON.stringify(context.messages));
          return step(context);
        }),
      );
    },
    id() {
      // The socket file <id>-<pid>.sock is the whole registration; the id derives from session id + pid.
      const id = deriveId(session.sessionId, process.pid);
      const entry = listSockets(busDir).find((e) => e.id === id);
      assert.ok(entry, `${name} has a socket`);
      assert.equal(entry.pid, process.pid);
      assert.equal(entry.path, join(busDir, socketFileName(id, process.pid)));
      return id;
    },
  };
  session.subscribe((event) => {
    if (event.type === "agent_start") node.runs++;
  });
  nodes.push(node);
  // What the rpc mode does at startup: bind the extensions, which emits session_start.
  await session.bindExtensions({ mode: "rpc" });
  session.setSessionName(name);
  return node;
}

interface ToolResultLike {
  role: "toolResult";
  toolName: string;
  isError: boolean;
  content: { type: string; text?: string }[];
  details?: Record<string, unknown>;
}

function toolResults(node: Node, toolName: string): ToolResultLike[] {
  return (node.session.messages as unknown as { role: string }[]).filter(
    (m): m is ToolResultLike => m.role === "toolResult" && (m as ToolResultLike).toolName === toolName,
  );
}

function busMessages(node: Node): { content: unknown; details: { note: { id: string; hops: number; content: string }; wake: string; reason?: string } }[] {
  return (node.session.messages as unknown as { role: string; customType?: string }[]).filter(
    (m) => m.role === "custom" && m.customType === "session-bus.message",
  ) as never;
}

const textOf = (result: ToolResultLike): string => result.content.map((c) => c.text ?? "").join("");

describe("two real Pi sessions on one bus", () => {
  it("(1) A's session_send wakes idle B: B's model request contains the note and A's tool result says 'started'", async () => {
    const a = await makeNode("alpha");
    const b = await makeNode("beta");
    assert.notEqual(a.id(), b.id());

    b.script([() => say("Understood, I will look at the schema.")]);
    a.script([
      () => call("session_list", {}),
      () => call("session_send", { to: "beta", content: "Please review the API schema in docs/schema.md" }),
      () => say("I told beta."),
    ]);

    await a.session.prompt("Tell beta to review the schema");
    await waitFor(() => b.requests.length >= 1, "B's turn to start");
    await b.session.waitForIdle();

    // B's model request contains the framed note.
    assert.equal(b.requests.length, 1, "B ran exactly one model request");
    assert.equal(b.runs, 1);
    const request = b.requests[0]!;
    assert.ok(request.includes("Please review the API schema in docs/schema.md"), "note content in B's request");
    assert.ok(request.includes("[session-bus message · from \\\"alpha\\\""), "framed header in B's request");
    assert.ok(request.includes(`(id ${a.id()}, cwd `), "sender id in B's request");
    assert.ok(request.includes("hop 1/4]"), "hop accounting in the header");
    assert.ok(request.includes("not from your user"), "untrusted-peer notice");
    assert.ok(request.includes(`to \\\"${a.id()}\\\", replyTo`), "reply instructions");

    // A's tool results: the listing showed B, the send reports 'started'.
    const [list] = toolResults(a, "session_list");
    assert.ok(list && !list.isError);
    assert.match(textOf(list), new RegExp(`- ${b.id()} "beta" · cwd .*cwd-beta · idle`));
    const [send] = toolResults(a, "session_send");
    assert.ok(send && !send.isError, "session_send succeeded");
    assert.match(textOf(send), /wake: started/);
    assert.match(textOf(send), /hop 1\/4/);
    assert.equal(send.details?.["wake"], "started");
    assert.equal(send.details?.["to"], b.id());

    // The message is part of B's session as a custom message.
    const received = busMessages(b);
    assert.equal(received.length, 1);
    assert.equal(received[0]!.details.wake, "started");
    assert.equal(received[0]!.details.note.hops, 1);
    assert.equal(received[0]!.details.note.id, send.details?.["msgId"]);
  });

  it("(2) a note arriving during B's slow tool does not abort it and reaches B's next request in the same run", async () => {
    const slowStarted = deferred();
    const slowRelease = deferred();
    let slowAbortedAtEnd: boolean | undefined;
    const slowTool = defineTool({
      name: "slow_tool",
      label: "Slow tool",
      description: "Waits until the test releases it",
      parameters: Type.Object({}),
      async execute(_toolCallId, _params, signal) {
        slowStarted.resolve();
        await slowRelease.promise;
        slowAbortedAtEnd = signal?.aborted;
        return { content: [{ type: "text", text: "slow tool finished" }], details: {} };
      },
    });

    const a = await makeNode("alpha");
    const b = await makeNode("beta", {}, [slowTool]);
    b.script([() => call("slow_tool", {}), () => say("Done, and I saw the heads-up.")]);
    a.script([
      () => call("session_send", { to: b.id(), content: "Heads-up: the schema moved to docs/v2/schema.md" }),
      () => say("Sent."),
    ]);

    const bRun = b.session.prompt("Run the slow tool");
    await slowStarted.promise; // B is now inside its tool call
    assert.equal(b.session.isStreaming, true);

    await a.session.prompt("Tell beta about the schema move");
    const [send] = toolResults(a, "session_send");
    assert.ok(send && !send.isError);
    assert.match(textOf(send), /wake: queued/, "B is busy: the note is queued, not a new turn");
    assert.equal(send.details?.["wake"], "queued");

    // The note arrived but the tool is untouched and B has made no new model request yet.
    assert.equal(b.requests.length, 1);
    assert.equal(slowAbortedAtEnd, undefined);
    assert.equal(busMessages(b).length, 0, "steered messages join the transcript at the next turn boundary");

    slowRelease.resolve();
    await bRun;
    await b.session.waitForIdle();

    assert.equal(slowAbortedAtEnd, false, "the slow tool's signal was never aborted");
    const [slowResult] = toolResults(b, "slow_tool");
    assert.ok(slowResult && !slowResult.isError, "the slow tool completed normally");
    assert.equal(textOf(slowResult), "slow tool finished");

    assert.equal(b.runs, 1, "the note did not start a second run");
    assert.equal(b.requests.length, 2);
    assert.ok(!b.requests[0]!.includes("Heads-up"), "not in the first request");
    assert.ok(b.requests[1]!.includes("Heads-up: the schema moved to docs/v2/schema.md"), "in B's next request");
    assert.ok(b.requests[1]!.includes("slow tool finished"), "together with the tool result");
    const received = busMessages(b);
    assert.equal(received.length, 1);
    assert.equal(received[0]!.details.wake, "queued");
  });

  it("(3) a reply chain stops waking once the hop limit is passed", async () => {
    const MAX_HOPS = 2;
    const gateB = deferred();
    const a = await makeNode("alpha", { maxHops: MAX_HOPS });
    const b = await makeNode("beta", { maxHops: MAX_HOPS });
    const msgIdIn = (request: string): string => {
      const match = /· msg ([0-9a-f-]{36}) ·/.exec(request);
      assert.ok(match, "a msg id in the request");
      return match[1]!;
    };

    a.script([
      // hop 1: A -> B
      () => call("session_send", { to: b.id(), content: "Question: which port does the API use?" }),
      () => say("Asked beta."),
      // hop 2 arrives from B and wakes A; A answers with hop 3, which must not wake B.
      (context) =>
        call("session_send", {
          to: b.id(),
          content: "Thanks, one more question: and the database port?",
          replyTo: msgIdIn(JSON.stringify(context.messages)),
        }),
      () => say("Done; not waiting for more."),
    ]);
    b.script([
      // B's first model request is held until A's own run has finished, so the order is deterministic.
      async (context) => {
        await gateB.promise;
        return call("session_send", {
          to: a.id(),
          content: "The API listens on port 8080.",
          replyTo: msgIdIn(JSON.stringify(context.messages)),
        });
      },
      () => say("Replied."),
    ]);

    await a.session.prompt("Ask beta which port the API uses");
    await waitFor(() => b.session.isStreaming, "B to be woken");
    const [first] = toolResults(a, "session_send");
    assert.equal(first?.details?.["wake"], "started");
    assert.equal(first?.details?.["hops"], 1);

    gateB.resolve();
    await waitFor(() => a.requests.length >= 4 && toolResults(a, "session_send").length === 2, "A's second send");
    await a.session.waitForIdle();
    await b.session.waitForIdle();
    await sleep(250); // a wrongly woken session would start a request by now

    // Hop 2 (B -> A) woke A; hop 3 (A -> B) was delivered but suppressed.
    const results = toolResults(a, "session_send");
    assert.equal(results.length, 2);
    assert.equal(results[1]!.details?.["hops"], 3);
    assert.equal(results[1]!.details?.["wake"], "suppressed");
    assert.equal(results[1]!.details?.["reason"], "hop_limit");
    assert.match(textOf(results[1]!), /hop 3\/2/);
    assert.match(textOf(results[1]!), /suppressed \(hop_limit/);

    assert.equal(a.requests.length, 4, "A: prompt, tool result, B's reply (woken), tool result");
    assert.equal(a.runs, 2);
    assert.equal(b.requests.length, 2, "B: the first note and its own tool result; the hop-3 note did not start anything");
    assert.equal(b.runs, 1);

    const reply = busMessages(a);
    assert.equal(reply.length, 1);
    assert.equal(reply[0]!.details.wake, "started");
    assert.equal(reply[0]!.details.note.hops, 2);

    const atB = busMessages(b);
    assert.deepEqual(atB.map((m) => [m.details.note.hops, m.details.wake, m.details.reason]), [
      [1, "started", undefined],
      [3, "suppressed", "hop_limit"],
    ]);
    assert.match(String(atB[1]!.content), /\nAuto-wake suppressed: hop_limit\.\n/);
  });

  it("an unreachable id and the socket are cleaned up when a session shuts down", async () => {
    const a = await makeNode("alpha");
    const b = await makeNode("beta");
    const bId = b.id();
    assert.equal(listSockets(busDir).length, 2);
    await b.session.extensionRunner.emit({ type: "session_shutdown", reason: "quit" });
    assert.deepEqual(listSockets(busDir).map((e) => e.id), [a.id()]);
    a.script([() => call("session_send", { to: bId, content: "are you there?" }), () => say("ok")]);
    await a.session.prompt("ping beta");
    const [send] = toolResults(a, "session_send");
    assert.ok(send?.isError, "sending to a vanished session is a tool error");
    assert.match(textOf(send), /no live session matches/);
  });

  it("repository queue: A's queue_done releases at settle, B's own deferred prompt runs in B and its model sees the turn notice", async () => {
    const repo = join(tmp, "shared-repo");
    mkdirSync(repo); execFileSync("git", ["init", "--quiet", repo]);
    const a = await makeNode("alpha", { queuePollMs: 20 }, [], repo);
    const b = await makeNode("beta", { queuePollMs: 60_000 }, [], repo);
    const finish = deferred();
    a.script([async () => { await finish.promise; return call("queue_done", {}); }, () => say("A's task is complete.")]);
    b.script([() => say("B has completed its own task.")]);
    await a.session.prompt("/queue implement A's change");
    await waitFor(() => a.requests.length === 1, "A to hold and run");
    await b.session.prompt("/queue on");
    await b.session.prompt("implement B's independent change");
    assert.equal(b.requests.length, 0, "B's user input is handled without running the model");
    const file = createQueueStore({ busDir, repo }).read();
    assert.deepEqual(file.entries.map(e => e.state), ["active", "waiting"]);
    assert.deepEqual(file.entries.map(e => e.endpointId), [a.id(), b.id()]);
    finish.resolve();
    await a.session.waitForIdle();
    await waitFor(() => b.requests.length === 1, "B's deferred user prompt to run after handoff");
    await b.session.waitForIdle();
    assert.match(textOf(toolResults(a, "queue_done")[0]!), /when this run ends/);
    assert.ok(b.requests[0]!.includes("implement B's independent change"));
    assert.ok(!b.requests[0]!.includes("implement A's change"), "the deferred task belongs to B, not A");
    assert.ok((b.session.messages as unknown as { role: string; customType?: string }[]).some(m => m.role === "custom" && m.customType === "session-bus.queue-turn"), "the turn notice is a separate extension-authored message");
    assert.ok(b.requests[0]!.includes(`holds the repository work-queue turn for ${repo}`));
    assert.ok(b.requests[0]!.includes("Call queue_done"));
    assert.ok(b.requests[0]!.includes("not asking or waiting for the user's reply"));
    assert.equal(b.runs, 1);
  });

  it("repository queue: multiple deferred prompts replay in order as real streaming follow-ups", async () => {
    const repo = join(tmp, "shared-repo");
    mkdirSync(repo); execFileSync("git", ["init", "--quiet", repo]);
    const a = await makeNode("alpha", {}, [], repo);
    const b = await makeNode("beta", { queuePollMs: 60_000 }, [], repo);
    a.script([() => say("Please give me the next part of the task.")]);
    b.script([() => say("First done."), () => say("Second done."), () => say("Third done.")]);
    await a.session.prompt("/queue hold A's task");
    await waitFor(() => a.requests.length === 1, "A to start"); await a.session.waitForIdle();
    await b.session.prompt("/queue on");
    for (const text of ["first deferred task", "second deferred task", "third deferred task"]) await b.session.prompt(text);
    assert.equal(b.requests.length, 0);
    await a.session.prompt("/queue done");
    await waitFor(() => b.requests.length === 3, "all three deferred prompts to run");
    await b.session.waitForIdle();
    assert.ok(b.requests[0]!.includes("first deferred task"));
    assert.ok(!b.requests[0]!.includes("second deferred task"));
    assert.ok(b.requests[1]!.includes("second deferred task"));
    assert.ok(!b.requests[1]!.includes("third deferred task"));
    assert.ok(b.requests[2]!.includes("third deferred task"));
  });

});
