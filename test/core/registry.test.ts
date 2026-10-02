import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  BusDirError,
  deriveId,
  ensurePrivateDir,
  listEntries,
  prepareSocketPath,
  readEntry,
  removeEntry,
  resolveBusDir,
  writeEntry,
  type RegistryEntry,
} from "../../src/core/registry.ts";
import { makeTempDir, removeTempDir } from "./helpers.ts";

const mode = (path: string): number => statSync(path).mode & 0o777;

let tmp: string;
beforeEach(() => {
  tmp = makeTempDir("sb-reg");
});
afterEach(() => {
  removeTempDir(tmp);
});

function entry(id: string, overrides: Partial<RegistryEntry> = {}): RegistryEntry {
  return {
    v: 1,
    id,
    sessionId: `session-${id}`,
    pid: 4242,
    socket: join(tmp, `${id}.sock`),
    startedAt: "2026-10-02T10:00:00.000Z",
    ...overrides,
  };
}

describe("resolveBusDir", () => {
  it("prefers PI_SESSION_BUS_DIR and does not call getAgentDir", () => {
    const dir = resolveBusDir({
      env: { PI_SESSION_BUS_DIR: "/tmp/custom-bus" },
      getAgentDir: () => {
        throw new Error("must not be called");
      },
    });
    assert.equal(dir, "/tmp/custom-bus");
  });

  it("falls back to <agentDir>/session-bus", () => {
    assert.equal(resolveBusDir({ env: {}, getAgentDir: () => "/home/u/.pi/agent" }), "/home/u/.pi/agent/session-bus");
    assert.equal(
      resolveBusDir({ env: { PI_SESSION_BUS_DIR: "  " }, getAgentDir: () => "/home/u/.pi/agent" }),
      "/home/u/.pi/agent/session-bus",
    );
  });

  it("makes relative overrides absolute", () => {
    assert.ok(resolveBusDir({ env: { PI_SESSION_BUS_DIR: "rel/bus" }, getAgentDir: () => "/x" }).startsWith("/"));
  });
});

describe("ensurePrivateDir", () => {
  it("creates the directory (and parents) with mode 0700", () => {
    const dir = join(tmp, "a", "b", "bus");
    ensurePrivateDir(dir);
    assert.equal(mode(dir), 0o700);
  });

  it("tightens an existing directory we own to 0700", () => {
    const dir = join(tmp, "loose");
    mkdirSync(dir, { mode: 0o755 });
    chmodSync(dir, 0o755);
    ensurePrivateDir(dir);
    assert.equal(mode(dir), 0o700);
  });

  it("refuses a symbolic link, even to a directory we own", () => {
    const real = join(tmp, "real");
    mkdirSync(real, { mode: 0o700 });
    const link = join(tmp, "link");
    symlinkSync(real, link);
    assert.throws(
      () => ensurePrivateDir(link),
      (err) => err instanceof BusDirError && err.code === "symlink",
    );
    assert.equal(mode(real), 0o700);
  });

  it("refuses something that is not a directory", () => {
    const file = join(tmp, "file");
    writeFileSync(file, "x");
    assert.throws(
      () => ensurePrivateDir(file),
      (err) => err instanceof BusDirError && err.code === "not_directory",
    );
  });

  it("refuses a directory owned by another uid and leaves its mode alone", () => {
    const dir = join(tmp, "foreign");
    mkdirSync(dir, { mode: 0o755 });
    chmodSync(dir, 0o755);
    const me = process.getuid?.() ?? 0;
    assert.throws(
      () => ensurePrivateDir(dir, { uid: me + 1 }),
      (err) => err instanceof BusDirError && err.code === "foreign_owner",
    );
    assert.equal(mode(dir), 0o755);
  });
});

describe("deriveId", () => {
  it("is 8 lowercase hex characters and deterministic", () => {
    const id = deriveId("session-1", 100);
    assert.match(id, /^[0-9a-f]{8}$/);
    assert.equal(deriveId("session-1", 100), id);
  });

  it("differs per process, per session and per salt", () => {
    const base = deriveId("session-1", 100);
    assert.notEqual(deriveId("session-1", 101), base);
    assert.notEqual(deriveId("session-2", 100), base);
    assert.notEqual(deriveId("session-1", 100, 1), base);
    assert.notEqual(deriveId("session-1", 100, 1), deriveId("session-1", 100, 2));
    assert.match(deriveId("session-1", 100, "x"), /^[0-9a-f]{8}$/);
  });
});

describe("prepareSocketPath", () => {
  it("keeps short socket paths inside the bus dir", () => {
    const busDir = join(tmp, "bus");
    ensurePrivateDir(busDir);
    assert.equal(prepareSocketPath(busDir, "abcd1234"), join(busDir, "abcd1234.sock"));
  });

  it("falls back to a private dir under XDG_RUNTIME_DIR when the path exceeds 103 bytes", () => {
    const busDir = join(tmp, "x".repeat(110));
    const runtime = join(tmp, "run");
    mkdirSync(runtime, { mode: 0o755 });
    const path = prepareSocketPath(busDir, "abcd1234", { env: { XDG_RUNTIME_DIR: runtime } });
    const uid = process.getuid?.();
    assert.equal(path, join(runtime, `pi-session-bus-${uid}`, "abcd1234.sock"));
    assert.equal(mode(join(runtime, `pi-session-bus-${uid}`)), 0o700);
    assert.ok(Buffer.byteLength(path) <= 103);
  });

  it("uses os.tmpdir() when XDG_RUNTIME_DIR is unset", () => {
    const busDir = join(tmp, "y".repeat(110));
    const path = prepareSocketPath(busDir, "abcd1234", { env: {}, tmpdir: () => tmp });
    assert.equal(path, join(tmp, `pi-session-bus-${process.getuid?.()}`, "abcd1234.sock"));
  });

  it("applies the same checks to the fallback dir (symlink refused)", () => {
    const busDir = join(tmp, "z".repeat(110));
    const runtime = join(tmp, "run");
    mkdirSync(runtime);
    const target = join(tmp, "elsewhere");
    mkdirSync(target, { mode: 0o700 });
    symlinkSync(target, join(runtime, `pi-session-bus-${process.getuid?.()}`));
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", { env: { XDG_RUNTIME_DIR: runtime } }),
      (err) => err instanceof BusDirError && err.code === "symlink",
    );
  });

  it("fails clearly when even the fallback is too long", () => {
    const busDir = join(tmp, "w".repeat(110));
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", { env: { XDG_RUNTIME_DIR: join(tmp, "r".repeat(110)) } }),
      (err) => err instanceof BusDirError && err.code === "path_too_long",
    );
  });
});

describe("registry entries", () => {
  it("writes atomically with mode 0600 and leaves no temp files", () => {
    const e = entry("aaaa0001");
    writeEntry(tmp, e);
    const file = join(tmp, "aaaa0001.json");
    assert.equal(mode(file), 0o600);
    assert.deepEqual(JSON.parse(readFileSync(file, "utf8")), e);
    assert.deepEqual(readdirSync(tmp), ["aaaa0001.json"]);
    assert.deepEqual(readEntry(tmp, "aaaa0001"), e);
  });

  it("overwrites an existing entry in place", () => {
    writeEntry(tmp, entry("aaaa0001", { pid: 1 }));
    writeEntry(tmp, entry("aaaa0001", { pid: 2 }));
    assert.equal(readEntry(tmp, "aaaa0001")?.pid, 2);
    assert.deepEqual(readdirSync(tmp), ["aaaa0001.json"]);
  });

  it("lists valid entries oldest first and skips junk", () => {
    writeEntry(tmp, entry("bbbb0002", { startedAt: "2026-10-02T10:00:02.000Z" }));
    writeEntry(tmp, entry("aaaa0001", { startedAt: "2026-10-02T10:00:01.000Z" }));
    writeFileSync(join(tmp, "cccc0003.json"), "{not json");
    writeFileSync(join(tmp, "dddd0004.json"), JSON.stringify(entry("eeee0005"))); // id does not match the file name
    writeFileSync(join(tmp, "ffff0006.json"), JSON.stringify({ ...entry("ffff0006"), socket: "relative.sock" }));
    writeFileSync(join(tmp, "notes.json"), "{}");
    writeFileSync(join(tmp, ".aaaa0001.1.abcd.tmp"), "partial");
    assert.deepEqual(
      listEntries(tmp).map((e) => e.id),
      ["aaaa0001", "bbbb0002"],
    );
    assert.deepEqual(listEntries(join(tmp, "missing")), []);
  });

  it("removeEntry deletes the json and the socket path, tolerating missing files", () => {
    const e = entry("aaaa0001");
    writeEntry(tmp, e);
    writeFileSync(e.socket, "");
    removeEntry(tmp, e);
    assert.deepEqual(readdirSync(tmp), []);
    removeEntry(tmp, e); // idempotent
  });

  it("removeEntry also removes a socket in the fallback dir, but never an unrelated file", () => {
    const other = join(tmp, "other");
    mkdirSync(other);
    const fallbackSock = join(other, "aaaa0001.sock");
    const innocent = join(other, "important.txt");
    writeFileSync(fallbackSock, "");
    writeFileSync(innocent, "keep");
    writeEntry(tmp, entry("aaaa0001", { socket: fallbackSock }));
    removeEntry(tmp, { id: "aaaa0001", socket: fallbackSock });
    assert.equal(existsSync(fallbackSock), false);
    removeEntry(tmp, { id: "aaaa0001", socket: innocent });
    assert.equal(existsSync(innocent), true);
  });
});
