import assert from "node:assert/strict";
import { chmodSync, existsSync, mkdirSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createSessionBusExtension, USAGE, type SessionBusOptions } from "../../src/index.ts";
import { BUS_COMPLETIONS, completeBusArguments } from "../../src/completions.ts";
import {
  DEPENDENCY_CHANNEL,
  dependencyOutcome,
  formatDependencyReport,
  type DependencyProvider,
  type DependencyReport,
  type RunResult,
} from "../../src/dependencies.ts";
import {
  CHECK_NOTE,
  INSTALL_NOTE,
  checkSessionBus,
  createSessionBusDependencyProvider,
  installSessionBus,
  type SessionBusDependencyOptions,
  type SessionBusDoctorState,
} from "../../src/doctor.ts";
import { makeTempDir, removeTempDir } from "../core/helpers.ts";
import { FakeHost, startPeer, type FakeHostOptions, type Peer } from "./helpers.ts";

let tmp: string;
let busDir: string;
let binDir: string;
let repo: string;
let hosts: FakeHost[];
let peers: Peer[];
let calls: { command: string; args: string[]; cwd?: string }[];

beforeEach(() => {
  tmp = makeTempDir("sb-deps");
  busDir = join(tmp, "bus");
  binDir = join(tmp, "bin");
  repo = join(tmp, "repo");
  mkdirSync(binDir);
  mkdirSync(repo);
  hosts = [];
  peers = [];
  calls = [];
});

afterEach(async () => {
  for (const host of hosts) if (!host.dead) await host.shutdown();
  await Promise.all(peers.map((p) => p.endpoint.stop()));
  removeTempDir(tmp);
});

/** A fake `git` on PATH (never executed: the fake runner answers). */
function fakeGit(): string {
  const path = join(binDir, "git");
  writeFileSync(path, "#!/bin/sh\nexit 0\n");
  chmodSync(path, 0o755);
  return path;
}

/** Fake runner: git --version and rev-parse answer as configured; every call is recorded. */
function runner(over: { version?: Partial<RunResult>; toplevel?: Partial<RunResult> } = {}): NonNullable<SessionBusDependencyOptions["run"]> {
  return async (command, args, options = {}) => {
    calls.push({ command, args, ...(options.cwd === undefined ? {} : { cwd: options.cwd }) });
    const base: RunResult = { code: 0, stdout: "", stderr: "" };
    if (args[0] === "--version") return { ...base, stdout: "git version 2.45.0\n", ...over.version };
    if (args[0] === "rev-parse") return { ...base, stdout: `${options.cwd}\n`, ...over.toplevel };
    return { ...base, code: 1, stderr: `unexpected ${args.join(" ")}` };
  };
}

function deps(over: Partial<SessionBusDependencyOptions> = {}): SessionBusDependencyOptions {
  return { env: { PATH: binDir }, pathDirs: [binDir], run: runner(), tmpdir: () => tmp, ...over };
}

function state(over: Partial<SessionBusDoctorState> = {}): SessionBusDoctorState {
  return { platform: "linux", busDir: () => busDir, allowedModes: new Set(["tui", "rpc"]), endpoint: () => ({ running: true, id: "abcd1234" }), ...over };
}

function item(report: DependencyReport, name: string) {
  const found = report.items.find((i) => i.name === name);
  assert.ok(found, `item ${name} in ${JSON.stringify(report.items)}`);
  return found;
}

function makeHost(hostOptions: FakeHostOptions = {}, extensionOptions: SessionBusOptions = {}): FakeHost {
  const host = new FakeHost({ cwd: repo, ...hostOptions });
  createSessionBusExtension({ busDir, dependencies: deps(), ...extensionOptions })(host.pi);
  hosts.push(host);
  return host;
}

describe("session-bus doctor (check)", () => {
  it("reports a healthy setup with every prerequisite ok", async () => {
    fakeGit();
    mkdirSync(busDir, { mode: 0o700 });
    const report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    assert.equal(report.note, CHECK_NOTE);
    assert.deepEqual(report.items.map((i) => [i.name, i.status]), [
      ["platform", "ok"],
      ["bus directory", "ok"],
      ["socket path length", "ok"],
      ["receiving (run mode)", "ok"],
      ["git", "ok"],
      ["git work tree", "ok"],
    ]);
    assert.equal(item(report, "git").optional, true);
    assert.equal(item(report, "git work tree").optional, true);
    assert.equal(dependencyOutcome(report, "check"), "healthy");
    assert.equal(formatDependencyReport("pi-session-bus", report, "check").level, "info");
    // only read-only git probes ran
    assert.deepEqual(calls.map((c) => c.args), [["--version"], ["rev-parse", "--show-toplevel"]]);
  });

  it("is read-only: a missing bus dir is reported creatable but not created, a loose mode is not tightened", async () => {
    fakeGit();
    let report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    assert.equal(item(report, "bus directory").status, "ok");
    assert.match(item(report, "bus directory").detail!, /does not exist yet/);
    assert.equal(existsSync(busDir), false);
    mkdirSync(busDir, { mode: 0o755 });
    chmodSync(busDir, 0o755);
    report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    assert.match(item(report, "bus directory").detail!, /mode 0755/);
    assert.equal(statSync(busDir).mode & 0o777, 0o755);
  });

  it("warns, with an install action, when the optional git is missing (no runner call)", async () => {
    mkdirSync(busDir, { mode: 0o700 });
    const report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    const git = item(report, "git");
    assert.equal(git.status, "warn");
    assert.equal(git.optional, true);
    assert.match(git.action!, /Install git/);
    assert.equal(report.items.some((i) => i.name === "git work tree"), false);
    assert.deepEqual(calls, []);
    assert.equal(dependencyOutcome(report, "check"), "warning");
  });

  it("reports a broken git probe and a cwd outside a work tree with actions", async () => {
    fakeGit();
    mkdirSync(busDir, { mode: 0o700 });
    let report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps({ run: runner({ version: { code: 127, stdout: "", stderr: "git: broken" } }) }));
    assert.equal(item(report, "git").status, "warn");
    assert.match(item(report, "git").detail!, /git: broken/);
    assert.match(item(report, "git").action!, /--version by hand/);
    report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps({ run: runner({ toplevel: { code: 128, stdout: "", stderr: "fatal: not a git repository" } }) }));
    const tree = item(report, "git work tree");
    assert.equal(tree.status, "warn");
    assert.match(tree.action!, /git repository/);
  });

  it("refuses a symlinked bus dir with an action", async () => {
    fakeGit();
    const real = join(tmp, "real");
    mkdirSync(real, { mode: 0o700 });
    symlinkSync(real, busDir);
    const report = await checkSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    const dir = item(report, "bus directory");
    assert.equal(dir.status, "error");
    assert.match(dir.detail!, /symbolic link/);
    assert.match(dir.action!, /PI_SESSION_BUS_DIR/);
    assert.equal(dependencyOutcome(report, "check"), "abnormal");
  });

  it("reports a bus dir that cannot be created", { skip: process.getuid?.() === 0 ? "root can write anywhere" : false }, async () => {
    const locked = join(tmp, "locked");
    mkdirSync(locked, { mode: 0o500 });
    chmodSync(locked, 0o500);
    try {
      const report = await checkSessionBus(state({ busDir: () => join(locked, "bus") }), { mode: "rpc", cwd: repo }, deps());
      const dir = item(report, "bus directory");
      assert.equal(dir.status, "error");
      assert.match(dir.action!, /mkdir -p -m 700/);
    } finally {
      chmodSync(locked, 0o700);
    }
  });

  it("checks the socket path length, with the private fallback and an error when even that is too long", async () => {
    fakeGit();
    const long = join(tmp, "x".repeat(100));
    let report = await checkSessionBus(state({ busDir: () => long }), { mode: "rpc", cwd: repo }, deps());
    const socket = item(report, "socket path length");
    assert.equal(socket.status, "ok");
    assert.match(socket.detail!, /fallback/);
    assert.equal(existsSync(join(tmp, `pi-session-bus-${process.getuid?.() ?? "user"}`)), false, "check creates no fallback dir");
    report = await checkSessionBus(state({ busDir: () => long }), { mode: "rpc", cwd: repo }, deps({ tmpdir: () => long }));
    assert.equal(item(report, "socket path length").status, "error");
    assert.match(item(report, "socket path length").action!, /shorter path/);
  });

  it("reports the run mode: unverified when this mode does not receive, error when the endpoint is down", async () => {
    fakeGit();
    mkdirSync(busDir, { mode: 0o700 });
    let report = await checkSessionBus(state(), { mode: "print", cwd: repo }, deps());
    const rx = item(report, "receiving (run mode)");
    assert.equal(rx.status, "unverified");
    assert.match(rx.detail!, /print mode/);
    assert.match(rx.action!, /\/bus list/);
    report = await checkSessionBus(state({ endpoint: () => ({ running: false }) }), { mode: "tui", cwd: repo }, deps());
    assert.equal(item(report, "receiving (run mode)").status, "error");
    assert.match(item(report, "receiving (run mode)").action!, /\/reload/);
  });

  it("reports Windows as unsupported with an explanation", async () => {
    const report = await checkSessionBus(state({ platform: "win32" }), { mode: "tui", cwd: repo }, deps());
    assert.deepEqual(report.items.map((i) => [i.name, i.status]), [["platform", "error"]]);
    assert.match(report.items[0]!.action!, /Linux, macOS or inside WSL/);
    const install = await installSessionBus(state({ platform: "win32" }), { mode: "tui", cwd: repo }, deps());
    assert.equal(install.items[0]!.status, "manual");
    assert.equal(dependencyOutcome(install, "install"), "manual action required");
  });
});

describe("session-bus install-deps", () => {
  it("installs nothing automatically, is repeatable and reports ok items as already available", async () => {
    fakeGit();
    mkdirSync(busDir, { mode: 0o700 });
    for (let round = 0; round < 2; round++) {
      const report = await installSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
      assert.equal(report.note, INSTALL_NOTE);
      assert.ok(report.items.length > 0);
      assert.ok(report.items.every((i) => i.status === "ok" && /^already available/.test(i.detail!)), JSON.stringify(report.items));
      assert.equal(dependencyOutcome(report, "install"), "already available");
    }
    assert.ok(calls.every((c) => c.args[0] === "--version" || c.args[0] === "rev-parse"), JSON.stringify(calls));
  });

  it("turns missing prerequisites into manual steps with their actions", async () => {
    mkdirSync(busDir, { mode: 0o700 });
    const report = await installSessionBus(state(), { mode: "rpc", cwd: repo }, deps());
    const git = item(report, "git");
    assert.equal(git.status, "manual");
    assert.equal(git.optional, true);
    assert.match(git.action!, /Install git/);
    assert.equal(formatDependencyReport("pi-session-bus", report, "install").level, "warning");
  });

  it("reuses an in-flight install", async () => {
    fakeGit();
    const provider = createSessionBusDependencyProvider(state(), deps());
    const a = provider.install({ mode: "rpc", cwd: repo });
    const b = provider.install({ mode: "rpc", cwd: repo });
    assert.equal(a, b);
    await a;
    assert.notEqual(provider.install({ mode: "rpc", cwd: repo }), a);
  });
});

describe("dependency provider and /bus wiring", () => {
  it("answers a synchronous request on the umbrella channel with id session-bus", () => {
    const host = new FakeHost({ cwd: repo });
    const handlers = new Map<string, (data: unknown) => void>();
    (host.pi.events as unknown as { on: unknown }).on = (channel: string, handler: (data: unknown) => void) => {
      handlers.set(channel, handler);
      return () => undefined;
    };
    createSessionBusExtension({ busDir, dependencies: deps() })(host.pi);
    hosts.push(host);
    const provided: DependencyProvider[] = [];
    handlers.get(DEPENDENCY_CHANNEL)!({ version: 1, provide: (p: DependencyProvider) => provided.push(p) });
    assert.equal(provided.length, 1);
    assert.equal(provided[0]!.id, "session-bus");
    assert.equal(provided[0]!.label, "pi-session-bus");
    assert.equal(provided[0]!.command, "/bus");
    handlers.get(DEPENDENCY_CHANNEL)!({ version: 1 }); // malformed requests are ignored
  });

  it("/bus doctor and /bus install-deps notify the formatted report", async () => {
    fakeGit();
    const host = makeHost();
    await host.start();
    await host.runCommand("doctor");
    const doctor = host.lastNotification!;
    assert.match(doctor.message, /^pi-session-bus doctor: healthy\n/);
    assert.match(doctor.message, /\[ok\] receiving \(run mode\): rpc mode, endpoint [0-9a-f]{8}/);
    assert.equal(doctor.level, "info");
    await host.runCommand("  INSTALL-DEPS ");
    const install = host.lastNotification!;
    assert.match(install.message, /^pi-session-bus install-deps: already available\n/);
    assert.ok(install.message.includes(INSTALL_NOTE));
    assert.equal(install.level, "info");
    assert.equal(existsSync(join(busDir)), true, "the running endpoint created it, not the doctor");
  });

  it("/bus doctor explains an unsupported platform instead of the generic refusal", async () => {
    const host = makeHost({}, { platform: "win32" });
    await host.start();
    await host.runCommand("doctor");
    assert.match(host.lastNotification!.message, /^pi-session-bus doctor: abnormal\n.*\[error\] platform/s);
    assert.equal(host.lastNotification!.level, "error");
    await host.runCommand("list");
    assert.match(host.lastNotification!.message, /unsupported/);
  });

  it("keeps existing subcommands: send to a peer named doctor, list, usage for extra words", async () => {
    fakeGit();
    const host = makeHost();
    await host.start();
    const peer = await startPeer(busDir, "peer-doctor", { name: "doctor" });
    peers.push(peer);
    await host.runCommand("send doctor install-deps now");
    assert.equal(peer.notes[0]!.content, "install-deps now");
    await host.runCommand("list");
    assert.match(host.lastNotification!.message, /"doctor"/);
    for (const args of ["doctor extra", "install-deps --force"]) {
      await host.runCommand(args);
      assert.deepEqual(host.lastNotification, { message: USAGE, level: "warning" });
    }
  });

  it("completes doctor and install-deps", () => {
    assert.deepEqual(completeBusArguments("d")?.map((c) => c.value), ["doctor"]);
    assert.deepEqual(completeBusArguments("in")?.map((c) => c.value), ["install-deps"]);
    assert.ok(BUS_COMPLETIONS.some((c) => c.value === "send "));
  });
});
