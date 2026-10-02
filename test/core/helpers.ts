import { spawn } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { createConnection, createServer, type Server, type Socket } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

/** A server that accepts connections and never answers (for timeout tests). */
export function listenSilent(path: string): Promise<{ server: Server; close: () => Promise<void> }> {
  return new Promise((resolve, reject) => {
    const sockets = new Set<Socket>();
    const server = createServer((sock) => {
      sockets.add(sock);
      sock.on("error", () => {});
      sock.on("close", () => sockets.delete(sock));
    });
    server.once("error", reject);
    server.listen(path, () => {
      resolve({
        server,
        close: () =>
          new Promise<void>((done) => {
            for (const s of sockets) s.destroy();
            server.close(() => done());
          }),
      });
    });
  });
}
