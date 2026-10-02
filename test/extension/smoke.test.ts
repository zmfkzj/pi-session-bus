/**
 * Smoke test with the real installed `pi` binary (no model calls, no network).
 * Opt-in: SESSION_BUS_SMOKE=1 npm test   (or: SESSION_BUS_SMOKE=1 node --test test/extension/smoke.test.ts)
 *
 * It spawns `pi -ne -e <abs>/src/index.ts --mode rpc --no-session --offline` with PI_SESSION_BUS_DIR and
 * PI_CODING_AGENT_DIR pointing at a temp dir, so the real ~/.pi/agent is never touched. Set SESSION_BUS_PI
 * to use a different pi executable.
 */

import assert from "node:assert/strict";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { existsSync, mkdirSync, readdirSync, realpathSync, statSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";
import {
  createEndpoint,
  createNote,
  deriveId,
  FrameDecoder,
  helloProbe,
  listSockets,
  sendNote,
  socketFileName,
  type Endpoint,
  type NoteRequest,
  type SocketEntry,
} from "../../src/core/index.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";

const ENABLED = process.env.SESSION_BUS_SMOKE === "1";
const EXTENSION = resolve(dirname(fileURLToPath(import.meta.url)), "../../src/index.ts");
const mode = (path: string): number => statSync(path).mode & 0o777;

type Record_ = Record<string, unknown> & { type?: string; id?: string };

describe("smoke: real pi in rpc mode", { skip: ENABLED ? false : "opt-in: set SESSION_BUS_SMOKE=1" }, () => {
  let tmp: string;
  let busDir: string;
  let cwd: string;
  let child: ChildProcessWithoutNullStreams;
  let exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  let stderr = "";
  const records: Record_[] = [];
  let peer: Endpoint | undefined;
  let entry: SocketEntry;
  let requestSerial = 0;

  async function waitFor<T>(what: string, probe: () => T | undefined, timeoutMs = 20_000): Promise<T> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const value = probe();
      if (value !== undefined) return value;
      if (Date.now() > deadline) throw new Error(`timed out waiting for ${what}\nstderr: ${stderr}`);
      await new Promise((r) => setTimeout(r, 25));
    }
  }

  function rpc(command: Record<string, unknown>): Promise<Record_> {
    const id = `smoke-${++requestSerial}`;
    child.stdin.write(`${JSON.stringify({ id, ...command })}\n`);
    return waitFor(`the response to ${JSON.stringify(command)}`, () =>
      records.find((r) => r.type === "response" && r.id === id),
    );
  }

  before(async () => {
    tmp = makeTempDir("sb-smoke");
    busDir = join(tmp, "bus");
    cwd = join(tmp, "cwd");
    mkdirSync(cwd, { recursive: true });
    mkdirSync(join(tmp, "agent"), { recursive: true });
    child = spawn(
      process.env.SESSION_BUS_PI ?? "pi",
      ["-ne", "-e", EXTENSION, "--mode", "rpc", "--no-session", "--offline"],
      {
        cwd,
        env: { ...process.env, PI_SESSION_BUS_DIR: busDir, PI_CODING_AGENT_DIR: join(tmp, "agent") },
        stdio: ["pipe", "pipe", "pipe"],
      },
    );
    exited = new Promise((resolveExit) => child.once("exit", (code, signal) => resolveExit({ code, signal })));
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString("utf8");
    });
    const decoder = new FrameDecoder(16 * 1024 * 1024);
    child.stdout.on("data", (chunk: Buffer) => {
      for (const frame of decoder.push(chunk).frames) records.push(JSON.parse(frame) as Record_);
    });
  });

  after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      child.kill("SIGKILL");
      await exited;
    }
    await peer?.stop();
    removeTempDir(tmp);
  });

  it("publishes a socket <id>-<pid>.sock (0600) and nothing else in the private bus dir (0700)", async () => {
    entry = await waitFor("the socket", () => listSockets(busDir)[0]);
    assert.equal(entry.pid, child.pid);
    assert.match(entry.id, /^[0-9a-f]{8}$/);
    assert.equal(entry.path, join(busDir, `${entry.id}-${child.pid}.sock`));
    assert.equal(entry.path, join(busDir, socketFileName(entry.id, entry.pid)));
    assert.equal(mode(busDir), 0o700);
    assert.equal(mode(entry.path), 0o600);
    assert.equal(listSockets(busDir).length, 1);
    assert.deepEqual(readdirSync(busDir), [`${entry.id}-${child.pid}.sock`], "the socket is the whole registry (no .json)");
  });

  it("answers a hello probe with live data", async () => {
    const hello = await helloProbe(entry.path);
    assert.equal(hello.id, entry.id);
    assert.equal(hello.id, deriveId(hello.sessionId, hello.pid), "the id derives from session id + pid");
    assert.equal(hello.pid, child.pid);
    assert.equal(hello.cwd, realpathSync(cwd));
    assert.equal(hello.receiving, true);
    assert.equal(hello.busy, false);
    assert.equal(hello.autoWake, true);
  });

  it("delivers a hops>4 note without starting a turn (visible through get_messages)", async () => {
    const note = createNote({
      from: { id: "5e0d0c0d", sessionId: "smoke-sender", name: "smoke-sender", cwd: "/smoke", replyable: true },
      to: entry.id,
      content: "smoke: this note is past the hop limit",
      hops: 5,
    });
    const response = await sendNote(entry.path, note);
    assert.deepEqual(response, { v: 1, ok: true, status: "delivered", wake: "suppressed", reason: "hop_limit" });

    const reply = await rpc({ type: "get_messages" });
    assert.equal(reply["success"], true);
    const messages = (reply["data"] as { messages: Record<string, unknown>[] }).messages;
    assert.equal(messages.length, 1, `exactly the custom message, no turn: ${JSON.stringify(messages).slice(0, 400)}`);
    const message = messages[0]!;
    assert.equal(message["role"], "custom");
    assert.equal(message["customType"], "session-bus.message");
    const content = typeof message["content"] === "string" ? message["content"] : JSON.stringify(message["content"]);
    assert.ok(content.includes("smoke: this note is past the hop limit"));
    assert.ok(content.includes("hop 5/4"));
    assert.ok(content.includes("Auto-wake suppressed: hop_limit."));
    assert.equal(records.some((r) => r.type === "agent_start"), false, "no agent run was started");
    assert.equal(records.some((r) => r.type === "message_update"), false);
  });

  it("/bus list and /bus send work through the real extension runner", async () => {
    const received: NoteRequest[] = [];
    peer = createEndpoint({
      busDir,
      sessionId: "smoke-peer-session",
      getPeerInfo: () => ({ name: "smoke-peer", cwd: "/smoke/peer", busy: false, autoWake: true }),
      onNote: (note) => {
        received.push(note);
        return { v: 1, ok: true, status: "delivered", wake: "started" };
      },
    });
    await peer.start();

    const notifications = (): string[] =>
      records.filter((r) => r.type === "extension_ui_request" && r["method"] === "notify").map((r) => String(r["message"]));

    await rpc({ type: "prompt", message: "/bus list" });
    const listing = await waitFor("the /bus list notification", () => notifications().find((m) => m.startsWith("This session:")));
    assert.ok(listing.includes(`id ${entry.id}`), listing);
    assert.ok(listing.includes(`- ${peer!.id} "smoke-peer"`), listing);

    await rpc({ type: "prompt", message: `/bus send smoke-peer hello from the real pi` });
    const sent = await waitFor("the peer to receive the note", () => received[0]);
    assert.equal(sent.content, "hello from the real pi");
    assert.equal(sent.hops, 1);
    assert.equal(sent.wake, true);
    assert.equal(sent.from.id, entry.id);
    assert.equal(sent.from.replyable, true);
    assert.equal(sent.to, peer!.id);
    await waitFor("the send confirmation", () => notifications().find((m) => m.startsWith("Sent msg")));

    await rpc({ type: "prompt", message: "/bus nonsense" });
    await waitFor("the usage notification", () =>
      notifications().find((m) => m === "Usage: /bus [list] | /bus send <to> <text> | /bus wake on|off"),
    );
    assert.equal(records.some((r) => r.type === "agent_start"), false, "still no agent run");
  });

  it("closing stdin shuts down cleanly and removes the socket", async () => {
    child.stdin.end();
    let timer: NodeJS.Timeout | undefined;
    const result = await Promise.race([
      exited,
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => reject(new Error(`pi did not exit\nstderr: ${stderr}`)), 20_000);
      }),
    ]).finally(() => clearTimeout(timer));
    assert.equal(result.code, 0, `exit code (stderr: ${stderr})`);
    assert.equal(existsSync(entry.path), false, "socket removed");
    assert.deepEqual(
      listSockets(busDir).map((e) => e.id),
      peer ? [peer.id] : [],
      "only the in-process test peer remains",
    );
    assert.deepEqual(
      readdirSync(busDir),
      peer ? [`${peer.id}-${process.pid}.sock`] : [],
      "no other file is left (no .json)",
    );
  });
});
