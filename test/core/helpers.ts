import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { encodeFrame, FrameDecoder, type PeerInfo } from "../../src/core/protocol.ts";

/** Short temp dirs (Unix socket paths are limited to ~103 bytes). Never touches ~/.pi. */
export function makeTempDir(label = "sb"): string {
  return mkdtempSync(join(tmpdir(), `${label}-`));
}

export function removeTempDir(dir: string): void {
  rmSync(dir, { recursive: true, force: true });
}

export interface RawResult {
  /** Bytes received before the connection ended. */
  received: string;
  /** True when the server closed the connection. */
  closed: boolean;
  /** True when the helper gave up waiting (connection still open). */
  timedOut: boolean;
}

/** Connect, write raw bytes, collect everything until the peer closes or `waitMs` passes. */
export function rawExchange(socketPath: string, payload: Buffer | string | undefined, waitMs = 1500): Promise<RawResult> {
  return new Promise((resolve) => {
    const chunks: Buffer[] = [];
    const sock = createConnection(socketPath);
    let done = false;
    const finish = (closed: boolean, timedOut: boolean): void => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      sock.destroy();
      resolve({ received: Buffer.concat(chunks).toString("utf8"), closed, timedOut });
    };
    const timer = setTimeout(() => finish(false, true), waitMs);
    sock.on("data", (c) => chunks.push(c));
    sock.on("error", () => finish(true, false));
    sock.on("close", () => finish(true, false));
    if (payload !== undefined) sock.write(payload);
  });
}

/**
 * Leave a stale socket file behind: a child process binds `path` and is SIGKILLed, so the
 * file stays but nobody listens (connect => ECONNREFUSED).
 */
export async function makeStaleSocket(path: string): Promise<void> {
  const child = spawn(
    process.execPath,
    ["-e", "require('node:net').createServer().listen(process.argv[1], () => console.log('ready'))", path],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  await new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.stdout.on("data", (d: Buffer) => {
      if (d.toString().includes("ready")) resolve();
    });
    void exited.then(() => reject(new Error("stale-socket helper exited early")));
  });
  child.kill("SIGKILL");
  await exited;
}

/**
 * Leave a stale socket file named `<id>-<pid>.sock` with the pid of a child that bound it and was
 * SIGKILLed: the file stays, the pid is gone (kill(pid, 0) => ESRCH) and nobody listens.
 */
export async function makeKilledChildSocket(dir: string, id: string): Promise<{ pid: number; path: string }> {
  const child = spawn(
    process.execPath,
    [
      "-e",
      "const p = require('node:path').join(process.argv[1], process.argv[2] + '-' + process.pid + '.sock');" +
        "require('node:net').createServer().listen(p, () => console.log('ready ' + process.pid));",
      dir,
      id,
    ],
    { stdio: ["ignore", "pipe", "inherit"] },
  );
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  const pid = child.pid;
  if (pid === undefined) throw new Error("could not spawn the socket helper");
  await new Promise<void>((resolve, reject) => {
    child.once("error", reject);
    child.stdout.on("data", (d: Buffer) => {
      if (d.toString().includes(`ready ${pid}`)) resolve();
    });
    void exited.then(() => reject(new Error("socket helper exited early")));
  });
  child.kill("SIGKILL");
  await exited;
  return { pid, path: join(dir, `${id}-${pid}.sock`) };
}

/** A pid that is not in use: `process.kill(pid, 0)` throws ESRCH (2^22-1, the Linux maximum, or the next free one below). */
export function findDeadPid(): number {
  for (let pid = 2 ** 22 - 1; pid > 2 ** 22 - 100; pid--) {
    try {
      process.kill(pid, 0);
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === "ESRCH") return pid;
    }
  }
  throw new Error("no unused pid found");
}

export interface TestServer {
  server: Server;
  /** Connections accepted so far (a prune that must not connect keeps this at 0). */
  connections: () => number;
  close: () => Promise<void>;
}

export function listenWith(path: string, onConnection: (sock: Socket) => void): Promise<TestServer> {
  return new Promise((resolve, reject) => {
    const sockets = new Set<Socket>();
    let accepted = 0;
    const server = createServer((sock) => {
      accepted++;
      sockets.add(sock);
      sock.on("error", () => {});
      sock.on("close", () => sockets.delete(sock));
      onConnection(sock);
    });
    server.once("error", reject);
    server.listen(path, () => {
      resolve({
        server,
        connections: () => accepted,
        close: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.destroy();
            server.close(() => done());
          }),
      });
    });
  });
}

/** A server that accepts connections and never answers (for timeout tests). */
export function listenSilent(path: string): Promise<TestServer> {
  return listenWith(path, () => {});
}

/** A server that answers every hello with `peer` (use a `peer` that differs from the file name to test mismatches). */
export function listenHello(path: string, peer: PeerInfo): Promise<TestServer> {
  return listenWith(path, (sock) => {
    const decoder = new FrameDecoder(64 * 1024);
    sock.on("data", (chunk) => {
      if (decoder.push(chunk).frames.length > 0) sock.end(encodeFrame({ v: 1, ok: true, peer }));
    });
  });
}

export function peerInfo(id: string, pid: number, over: Partial<PeerInfo> = {}): PeerInfo {
  return { id, sessionId: `session-${id}`, cwd: "/work/fake", pid, busy: false, autoWake: true, receiving: true, ...over };
}
