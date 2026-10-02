import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  readdirSync,
  readFileSync,
  rmSync,
  statSync,
  symlinkSync,
  utimesSync,
  writeFileSync,
} from "node:fs";
import { basename, dirname, join, resolve } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import {
  BusDirError,
  deriveId,
  ensurePrivateDir,
  fallbackDir,
  listSockets,
  parseSocketFileName,
  prepareSocketPath,
  removeSocket,
  resolveBusDir,
  socketFileName,
  type SocketEntry,
} from "../../src/core/registry.ts";
import { listenSilent, makeTempDir, removeTempDir, type TestServer } from "./helpers.ts";

const mode = (path: string): number => statSync(path).mode & 0o777;

let tmp: string;
beforeEach(() => {
  tmp = makeTempDir("sb-reg");
});
afterEach(() => {
  removeTempDir(tmp);
});

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

describe("socketFileName / parseSocketFileName", () => {
  it("builds <id>-<pid>.sock", () => {
    assert.equal(socketFileName("abcd1234", 4242), "abcd1234-4242.sock");
  });

  it("round-trips", () => {
    for (const [id, pid] of [
      ["abcd1234", 1],
      ["00000000", 4242],
      ["ffffffff", 4_194_303],
      [deriveId("s", 7), 99_999],
    ] as const) {
      assert.deepEqual(parseSocketFileName(socketFileName(id, pid)), { id, pid });
    }
  });

  it("rejects everything that is not exactly <8 lowercase hex>-<pid without leading zero>.sock", () => {
    for (const name of [
      "abc.sock", // legacy / too short, no pid
      "abcd1234.sock", // legacy format
      "0123456789-0.sock", // id too long and pid 0
      "abcd1234-0.sock", // pid 0
      "abcd1234-042.sock", // leading zero
      "abcd1234-.sock",
      "abcd1234--5.sock",
      "abcd1234-5x.sock",
      "abcd1234-5.sock.bak",
      "abcd1234-5.json",
      "abcd1234-5",
      ".json",
      "ABCD1234-4242.sock", // uppercase id
      "abcg1234-4242.sock", // not hex
      "abcd123-4242.sock", // 7 characters
      "abcd12345-4242.sock", // 9 characters
      " abcd1234-4242.sock",
      "abcd1234-4242.sock\n",
      "abcd1234-99999999999999999999.sock", // not a safe integer
      "",
    ]) {
      assert.equal(parseSocketFileName(name), undefined, JSON.stringify(name));
    }
  });
});

describe("fallbackDir", () => {
  const hash8 = (dir: string): string => createHash("sha256").update(dir).digest("hex").slice(0, 8);
  const uid = process.getuid?.();

  it("is <runtime>/pi-session-bus-<uid>/<sha256-8 of the absolute busDir>", () => {
    const busDir = join(tmp, "bus-one");
    const dir = fallbackDir(busDir, { env: { XDG_RUNTIME_DIR: join(tmp, "run") } });
    assert.equal(dir, join(tmp, "run", `pi-session-bus-${uid}`, hash8(busDir)));
    assert.match(basename(dir), /^[0-9a-f]{8}$/);
    assert.ok(dir.includes(hash8(busDir)));
  });

  it("differs for two busDirs and is stable for the same one", () => {
    const options = { env: { XDG_RUNTIME_DIR: join(tmp, "run") } };
    const a = fallbackDir(join(tmp, "bus-a"), options);
    const b = fallbackDir(join(tmp, "bus-b"), options);
    assert.notEqual(a, b);
    assert.notEqual(basename(a), basename(b));
    assert.equal(dirname(a), dirname(b)); // same shared per-user directory
    assert.equal(fallbackDir(join(tmp, "bus-a"), options), a);
  });

  it("hashes the absolute path: a relative busDir and its absolute form agree", () => {
    const options = { env: { XDG_RUNTIME_DIR: join(tmp, "run") } };
    assert.equal(fallbackDir("rel/bus", options), fallbackDir(resolve("rel/bus"), options));
  });

  it("uses os.tmpdir() when XDG_RUNTIME_DIR is unset or empty, and the uid option", () => {
    const busDir = join(tmp, "bus");
    const expected = join(tmp, "pi-session-bus-1234", hash8(busDir));
    assert.equal(fallbackDir(busDir, { env: {}, tmpdir: () => tmp, uid: 1234 }), expected);
    assert.equal(fallbackDir(busDir, { env: { XDG_RUNTIME_DIR: "" }, tmpdir: () => tmp, uid: 1234 }), expected);
  });

  it("only computes the path: nothing is created", () => {
    const runtime = join(tmp, "run");
    fallbackDir(join(tmp, "bus"), { env: { XDG_RUNTIME_DIR: runtime } });
    assert.equal(existsSync(runtime), false);
  });
});

describe("prepareSocketPath", () => {
  it("keeps short socket paths inside the bus dir and creates nothing", () => {
    const busDir = join(tmp, "bus");
    const runtime = join(tmp, "run");
    assert.equal(prepareSocketPath(busDir, "abcd1234", 4242, { env: { XDG_RUNTIME_DIR: runtime } }), join(busDir, "abcd1234-4242.sock"));
    assert.equal(existsSync(runtime), false);
    assert.equal(existsSync(busDir), false);
  });

  it("falls back to <runtime>/pi-session-bus-<uid>/<hash> (both levels 0700) when the path exceeds 103 bytes", () => {
    const busDir = join(tmp, "x".repeat(110));
    const runtime = join(tmp, "run");
    mkdirSync(runtime, { mode: 0o755 });
    const options = { env: { XDG_RUNTIME_DIR: runtime } };
    const path = prepareSocketPath(busDir, "abcd1234", 4242, options);
    const dir = fallbackDir(busDir, options);
    assert.equal(path, join(dir, "abcd1234-4242.sock"));
    assert.equal(dirname(dirname(path)), join(runtime, `pi-session-bus-${process.getuid?.()}`));
    assert.equal(mode(dirname(dir)), 0o700);
    assert.equal(mode(dir), 0o700);
    assert.ok(Buffer.byteLength(path) <= 103);
  });

  it("uses the exact limit: 103 bytes stay in the bus dir, 104 move", () => {
    const name = socketFileName("abcd1234", 4242);
    const busDir = join(tmp, "b".repeat(103 - Buffer.byteLength(join(tmp, name)) - 1));
    assert.equal(Buffer.byteLength(join(busDir, name)), 103);
    const options = { env: { XDG_RUNTIME_DIR: join(tmp, "run") } };
    assert.equal(prepareSocketPath(busDir, "abcd1234", 4242, options), join(busDir, name));
    assert.equal(prepareSocketPath(`${busDir}b`, "abcd1234", 4242, options), join(fallbackDir(`${busDir}b`, options), name));
  });

  it("uses os.tmpdir() when XDG_RUNTIME_DIR is unset", () => {
    const busDir = join(tmp, "y".repeat(110));
    const options = { env: {}, tmpdir: () => tmp };
    const path = prepareSocketPath(busDir, "abcd1234", 4242, options);
    assert.equal(path, join(fallbackDir(busDir, options), "abcd1234-4242.sock"));
    assert.equal(dirname(dirname(path)), join(tmp, `pi-session-bus-${process.getuid?.()}`));
  });

  it("gives two bus dirs two different fallback dirs", () => {
    const options = { env: { XDG_RUNTIME_DIR: join(tmp, "run") } };
    const a = prepareSocketPath(join(tmp, "a".repeat(110)), "abcd1234", 4242, options);
    const b = prepareSocketPath(join(tmp, "c".repeat(110)), "abcd1234", 4242, options);
    assert.notEqual(dirname(a), dirname(b));
  });

  it("applies the same checks to both fallback levels (symlink refused)", () => {
    const busDir = join(tmp, "z".repeat(110));
    const runtime = join(tmp, "run");
    mkdirSync(runtime);
    const target = join(tmp, "elsewhere");
    mkdirSync(target, { mode: 0o700 });
    const options = { env: { XDG_RUNTIME_DIR: runtime } };
    const dir = fallbackDir(busDir, options);
    symlinkSync(target, dirname(dir)); // first level
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", 4242, options),
      (err) => err instanceof BusDirError && err.code === "symlink",
    );
    rmSync(dirname(dir));
    mkdirSync(dirname(dir), { mode: 0o700 });
    symlinkSync(target, dir); // second level (the hash dir)
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", 4242, options),
      (err) => err instanceof BusDirError && err.code === "symlink",
    );
    assert.deepEqual(readdirSync(target), []);
  });

  it("refuses a fallback dir owned by another uid", () => {
    const busDir = join(tmp, "o".repeat(110));
    const me = process.getuid?.() ?? 0;
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", 4242, { env: { XDG_RUNTIME_DIR: tmp }, uid: me + 1 }),
      (err) => err instanceof BusDirError && err.code === "foreign_owner",
    );
  });

  it("fails clearly when even the fallback is too long", () => {
    const busDir = join(tmp, "w".repeat(110));
    assert.throws(
      () => prepareSocketPath(busDir, "abcd1234", 4242, { env: { XDG_RUNTIME_DIR: join(tmp, "r".repeat(110)) } }),
      (err) => err instanceof BusDirError && err.code === "path_too_long",
    );
  });
});

describe("listSockets", () => {
  let servers: TestServer[];
  beforeEach(() => {
    servers = [];
  });
  afterEach(async () => {
    await Promise.all(servers.map((s) => s.close()));
  });

  /** A real Unix socket file at `path`. */
  async function bind(path: string): Promise<void> {
    servers.push(await listenSilent(path));
  }

  const names = (list: SocketEntry[]): string[] => list.map((e) => basename(e.path));

  it("lists <id>-<pid>.sock sockets with id, pid, absolute path and mtimeMs", async () => {
    const busDir = join(tmp, "bus");
    ensurePrivateDir(busDir);
    await bind(join(busDir, "aaaa0001-4242.sock"));
    const [entry, ...rest] = listSockets(busDir);
    assert.deepEqual(rest, []);
    assert.ok(entry);
    assert.deepEqual(Object.keys(entry).sort(), ["id", "mtimeMs", "path", "pid"]);
    assert.equal(entry.id, "aaaa0001");
    assert.equal(entry.pid, 4242);
    assert.equal(entry.path, join(busDir, "aaaa0001-4242.sock"));
    assert.equal(entry.mtimeMs, lstatSync(entry.path).mtimeMs);
  });

  it("reads both the bus dir and the fallback dir", async () => {
    const busDir = join(tmp, "bus");
    const runtime = join(tmp, "run");
    const options = { env: { XDG_RUNTIME_DIR: runtime } };
    ensurePrivateDir(busDir);
    const fallback = fallbackDir(busDir, options);
    ensurePrivateDir(dirname(fallback));
    ensurePrivateDir(fallback);
    await bind(join(busDir, "aaaa0001-4242.sock"));
    await bind(join(fallback, "bbbb0002-4243.sock"));
    const list = listSockets(busDir, options);
    assert.deepEqual(names(list).sort(), ["aaaa0001-4242.sock", "bbbb0002-4243.sock"]);
    assert.deepEqual(
      list.map((e) => [e.id, e.pid, dirname(e.path)]).sort(),
      [
        ["aaaa0001", 4242, busDir],
        ["bbbb0002", 4243, fallback],
      ],
    );
    // without the runtime dir the fallback is not read
    assert.deepEqual(names(listSockets(busDir, { env: { XDG_RUNTIME_DIR: join(tmp, "other-run") } })), ["aaaa0001-4242.sock"]);
  });

  it("reads the fallback dir of this busDir only (another bus dir has its own hash)", async () => {
    const runtime = join(tmp, "run");
    const options = { env: { XDG_RUNTIME_DIR: runtime } };
    const busA = join(tmp, "bus-a");
    const busB = join(tmp, "bus-b");
    const fallbackA = fallbackDir(busA, options);
    ensurePrivateDir(dirname(fallbackA));
    ensurePrivateDir(fallbackA);
    await bind(join(fallbackA, "aaaa0001-4242.sock"));
    assert.deepEqual(names(listSockets(busA, options)), ["aaaa0001-4242.sock"]);
    assert.deepEqual(listSockets(busB, options), []);
  });

  it("ignores regular files, symlinks and directories that carry a matching name", async () => {
    const busDir = join(tmp, "bus");
    ensurePrivateDir(busDir);
    await bind(join(busDir, "aaaa0001-4242.sock"));
    writeFileSync(join(busDir, "bbbb0002-4243.sock"), "not a socket");
    mkdirSync(join(busDir, "cccc0003-4244.sock"));
    symlinkSync(join(busDir, "aaaa0001-4242.sock"), join(busDir, "dddd0004-4245.sock"));
    symlinkSync(join(tmp, "nowhere"), join(busDir, "eeee0005-4246.sock")); // dangling
    assert.deepEqual(names(listSockets(busDir)), ["aaaa0001-4242.sock"]);
  });

  it("ignores sockets whose names do not match (legacy, uppercase, no pid, extra suffix)", async () => {
    const busDir = join(tmp, "bus");
    ensurePrivateDir(busDir);
    await bind(join(busDir, "aaaa0001-4242.sock"));
    for (const name of ["aaaa0002.sock", "AAAA0003-4242.sock", "aaaa0004-0.sock", "aaaa0005-4242.sock.old", "notes-4242.sock", "aaaa0006-4242.json"]) {
      await bind(join(busDir, name));
    }
    assert.deepEqual(names(listSockets(busDir)), ["aaaa0001-4242.sock"]);
  });

  it("ignores a fallback dir that is a symlink or owned by someone else", async () => {
    const busDir = join(tmp, "bus");
    const runtime = join(tmp, "run");
    const options = { env: { XDG_RUNTIME_DIR: runtime } };
    const real = join(tmp, "real");
    ensurePrivateDir(real);
    await bind(join(real, "aaaa0001-4242.sock"));
    const fallback = fallbackDir(busDir, options);
    ensurePrivateDir(dirname(fallback));
    symlinkSync(real, fallback); // hash level is a symlink
    assert.deepEqual(listSockets(busDir, options), []);

    rmSync(fallback);
    ensurePrivateDir(fallback);
    await bind(join(fallback, "bbbb0002-4243.sock"));
    assert.deepEqual(names(listSockets(busDir, options)), ["bbbb0002-4243.sock"]);
    const me = process.getuid?.() ?? 0;
    assert.deepEqual(listSockets(busDir, { ...options, uid: me + 1 }), []); // someone else's dir
  });

  it("returns oldest first (mtime, then id, then pid) and tolerates missing directories", async () => {
    const busDir = join(tmp, "bus");
    ensurePrivateDir(busDir);
    const files: [string, number][] = [
      ["cccc0003-10.sock", 1000],
      ["bbbb0002-11.sock", 3000],
      ["aaaa0001-12.sock", 3000],
      ["aaaa0001-9.sock", 3000],
      ["dddd0004-13.sock", 2000],
    ];
    for (const [name, seconds] of files) {
      const path = join(busDir, name);
      await bind(path);
      utimesSync(path, seconds, seconds);
    }
    assert.deepEqual(names(listSockets(busDir)), [
      "cccc0003-10.sock",
      "dddd0004-13.sock",
      "aaaa0001-9.sock",
      "aaaa0001-12.sock",
      "bbbb0002-11.sock",
    ]);
    assert.deepEqual(listSockets(join(tmp, "missing"), { env: { XDG_RUNTIME_DIR: join(tmp, "also-missing") } }), []);
  });
});

describe("removeSocket", () => {
  let servers: TestServer[];
  beforeEach(() => {
    servers = [];
  });
  afterEach(async () => {
    await Promise.all(servers.map((s) => s.close()));
  });

  it("unlinks a socket and reports it", async () => {
    const path = join(tmp, "aaaa0001-4242.sock");
    servers.push(await listenSilent(path));
    assert.equal(removeSocket(path), true);
    assert.equal(existsSync(path), false);
  });

  it("refuses (does not unlink) a regular file", () => {
    const path = join(tmp, "aaaa0001-4242.sock");
    writeFileSync(path, "precious");
    assert.equal(removeSocket(path), false);
    assert.equal(readFileSync(path, "utf8"), "precious");
  });

  it("refuses a directory and a symlink (even one pointing to a socket)", async () => {
    const dir = join(tmp, "adir.sock");
    mkdirSync(dir);
    assert.equal(removeSocket(dir), false);
    assert.equal(lstatSync(dir).isDirectory(), true);

    const real = join(tmp, "bbbb0002-4243.sock");
    servers.push(await listenSilent(real));
    const link = join(tmp, "cccc0003-4244.sock");
    symlinkSync(real, link);
    assert.equal(removeSocket(link), false);
    assert.equal(lstatSync(link).isSymbolicLink(), true);
    assert.equal(lstatSync(real).isSocket(), true);
  });

  it("returns false for a missing path without throwing", () => {
    assert.equal(removeSocket(join(tmp, "missing-4242.sock")), false);
    assert.equal(removeSocket(join(tmp, "no-such-dir", "x.sock")), false);
  });
});
