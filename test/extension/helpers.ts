import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { createEndpoint, type Endpoint, type NoteRequest, type NoteResponse } from "../../src/core/index.ts";

type AnyHandler = (event: unknown, ctx: unknown) => unknown;

export interface FakeTool {
  name: string;
  label: string;
  description: string;
  promptSnippet?: string;
  promptGuidelines?: string[];
  annotations?: { readOnlyHint?: boolean };
  parameters: unknown;
  execute(
    toolCallId: string,
    params: Record<string, unknown>,
    signal: AbortSignal | undefined,
    onUpdate: undefined,
    ctx: ExtensionContext,
  ): Promise<{ content: { type: string; text: string }[]; details: Record<string, unknown> }>;
}

export interface FakeCommand {
  description?: string;
  getArgumentCompletions?: (prefix: string) => { value: string; label: string; description?: string }[] | null;
  handler(args: string, ctx: ExtensionContext): Promise<void>;
}

export interface SentMessage {
  message: { customType: string; content: string; display: boolean; details: Record<string, unknown> };
  options: Record<string, unknown> | undefined;
}

export interface Notification {
  message: string;
  level: string | undefined;
}

export interface FakeHostOptions {
  mode?: ExtensionContext["mode"];
  hasUI?: boolean;
  sessionId?: string;
  cwd?: string;
  sessionName?: string;
}

/**
 * A fake Pi host: a recording ExtensionAPI plus an ExtensionContext. Like the real runtime, every
 * API/context call throws once the host is `dead` (after session_shutdown), and each such attempt is
 * counted in `callsAfterDeath` so tests can assert that nothing touched pi after shutdown.
 */
export class FakeHost {
  readonly handlers = new Map<string, AnyHandler[]>();
  readonly tools = new Map<string, FakeTool>();
  readonly commands = new Map<string, FakeCommand>();
  readonly sent: SentMessage[] = [];
  readonly userMessages: { content: string | unknown[]; options?: Record<string, unknown> }[] = [];
  /** `pi.events.emit` calls, in order. */
  readonly events: { channel: string; data: unknown }[] = [];
  /** Stands in for the images extension on its attachments channel (`pi-images:attachments`). */
  imageProvider?: (request: { text: string; existing: readonly unknown[] }) => unknown[];
  editorText = "";
  readonly notifications: Notification[] = [];
  readonly statuses: (string | undefined)[] = [];
  readonly queueStatuses: (string | undefined)[] = [];
  readonly callsAfterDeath: string[] = [];
  idle = true;
  sessionName: string | undefined;
  dead = false;
  readonly pi: ExtensionAPI;
  readonly ctx: ExtensionContext;

  constructor(options: FakeHostOptions = {}) {
    this.sessionName = options.sessionName;
    const guard = (name: string): void => {
      if (this.dead) {
        this.callsAfterDeath.push(name);
        throw new Error(`stale extension runtime: ${name}`);
      }
    };
    const pi = {
      on: (event: string, handler: AnyHandler) => {
        guard("pi.on");
        const list = this.handlers.get(event) ?? [];
        list.push(handler);
        this.handlers.set(event, list);
        return () => undefined;
      },
      registerTool: (tool: FakeTool) => {
        guard("pi.registerTool");
        this.tools.set(tool.name, tool);
      },
      registerCommand: (name: string, command: FakeCommand) => {
        guard("pi.registerCommand");
        this.commands.set(name, command);
      },
      sendMessage: (message: SentMessage["message"], options?: Record<string, unknown>) => {
        guard("pi.sendMessage");
        this.sent.push({ message, options });
      },
      sendUserMessage: (content: string | unknown[], options?: Record<string, unknown>) => {
        guard("pi.sendUserMessage");
        this.userMessages.push({ content, ...(options ? { options } : {}) });
      },
      events: {
        emit: (channel: string, data: unknown) => {
          guard("pi.events.emit");
          this.events.push({ channel, data });
          const request = data as { text: string; existing: readonly unknown[]; provide(images: unknown[]): void };
          if (channel === "pi-images:attachments" && this.imageProvider) request.provide(this.imageProvider(request));
        },
        on: () => {
          guard("pi.events.on");
          return () => undefined;
        },
      },
      getSessionName: () => {
        guard("pi.getSessionName");
        return this.sessionName;
      },
    };
    this.pi = pi as unknown as ExtensionAPI;

    const sessionId = options.sessionId ?? "11111111-aaaa-4bbb-8ccc-000000000001";
    const ctx = {
      mode: options.mode ?? "rpc",
      hasUI: options.hasUI ?? true,
      sessionManager: {
        getSessionId: () => {
          guard("ctx.sessionManager.getSessionId");
          return sessionId;
        },
      },
      isIdle: () => {
        guard("ctx.isIdle");
        return this.idle;
      },
      ui: {
        notify: (message: string, level?: string) => {
          guard("ctx.ui.notify");
          this.notifications.push({ message, level });
        },
        setStatus: (_key: string, text: string | undefined) => {
          guard("ctx.ui.setStatus");
          (_key === "session-bus-queue" ? this.queueStatuses : this.statuses).push(text);
        },
        getEditorText: () => { guard("ctx.ui.getEditorText"); return this.editorText; },
        setEditorText: (text: string) => { guard("ctx.ui.setEditorText"); this.editorText = text; },
      },
    };
    Object.defineProperty(ctx, "cwd", {
      enumerable: true,
      get: () => {
        guard("ctx.cwd");
        return options.cwd ?? "/work/project";
      },
    });
    this.ctx = ctx as unknown as ExtensionContext;
  }

  async fire(event: string, payload: Record<string, unknown> = {}): Promise<unknown> {
    let result: unknown;
    for (const handler of this.handlers.get(event) ?? []) {
      const value = await handler({ type: event, ...payload }, this.ctx);
      if (value !== undefined) result = value;
    }
    return result;
  }

  async start(reason = "startup"): Promise<void> {
    await this.fire("session_start", { reason });
  }

  /** Fire session_shutdown, then tear the "runtime" down (every later pi/ctx call throws). */
  async shutdown(reason = "quit"): Promise<void> {
    await this.fire("session_shutdown", { reason });
    this.dead = true;
  }

  tool(name: string): FakeTool {
    const tool = this.tools.get(name);
    if (!tool) throw new Error(`tool ${name} is not registered`);
    return tool;
  }

  runTool(name: string, params: Record<string, unknown>) {
    return this.tool(name).execute("call-1", params, undefined, undefined, this.ctx);
  }

  runCommand(args: string, name = "bus"): Promise<void> {
    const command = this.commands.get(name);
    if (!command) throw new Error(`/${name} is not registered`);
    return command.handler(args, this.ctx);
  }

  get lastNotification(): Notification | undefined {
    return this.notifications.at(-1);
  }
}

/** A plain core endpoint playing "another session": records the notes it receives. */
export interface Peer {
  endpoint: Endpoint;
  notes: NoteRequest[];
  name: string | undefined;
}

export async function startPeer(
  busDir: string,
  sessionId: string,
  options: { name?: string; onNote?: (note: NoteRequest) => NoteResponse | Promise<NoteResponse> } = {},
): Promise<Peer> {
  const notes: NoteRequest[] = [];
  const peer: Peer = { endpoint: undefined as unknown as Endpoint, notes, name: options.name };
  peer.endpoint = createEndpoint({
    busDir,
    sessionId,
    getPeerInfo: () => ({ ...(peer.name === undefined ? {} : { name: peer.name }), cwd: "/peer/cwd", busy: false, autoWake: true }),
    onNote: (note) => {
      notes.push(note);
      return options.onNote ? options.onNote(note) : { v: 1, ok: true, status: "delivered", wake: "started" };
    },
  });
  await peer.endpoint.start();
  return peer;
}
