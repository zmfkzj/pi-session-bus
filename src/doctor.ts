/**
 * `/bus doctor` and `/bus install-deps`: the session-bus provider of the dependency contract (./dependencies.ts).
 *
 * session-bus has no npm runtime packages (Node built-ins plus what Pi provides), but it has runtime prerequisites,
 * which check() reports as items:
 *   - platform          POSIX with Unix domain sockets (Windows: error; nothing to install, use Linux/macOS/WSL)
 *   - bus directory     the socket/registry dir exists (a real 0700 directory we own) or can be created
 *   - socket path       the socket path fits sun_path (103 bytes), directly or through the private fallback dir
 *   - receiving         this session's run mode receives notes (TUI/RPC) and its endpoint runs; other modes: unverified
 *   - git               (optional) needed by the /queue repository work queue
 *   - git work tree     (optional, per use) /queue works only inside a git work tree; checked for the current cwd
 *
 * check() is read-only: it never creates or chmods directories (unlike ensurePrivateDir) and bounds every git call
 * with a timeout. install() installs nothing automatically: it reports every missing prerequisite as "manual".
 */

import { accessSync, constants, lstatSync, statSync } from "node:fs";
import { tmpdir as osTmpdir } from "node:os";
import { dirname, join } from "node:path";
import { MAX_SOCKET_PATH_BYTES, fallbackDir, socketFileName } from "./core/index.ts";
import {
  findExecutable,
  outputTail,
  runProcess,
  type DependencyItem,
  type DependencyProvider,
  type DependencyReport,
} from "./dependencies.ts";

export const DEPENDENCY_ID = "session-bus";
export const DEPENDENCY_LABEL = "pi-session-bus";
export const DEPENDENCY_COMMAND = "/bus";
export const CHECK_NOTE = "No npm runtime dependencies (Node built-ins and Pi only); checked the runtime prerequisites below.";
export const INSTALL_NOTE =
  "No automatically installable dependencies: session-bus uses only Node built-ins and Pi. Missing prerequisites are listed as manual steps.";
/** Bound of every git probe. */
export const GIT_TIMEOUT_MS = 5_000;
/** Worst-case socket file name (8 hex chars + a 7-digit pid, Linux pid_max 4194304). */
const WORST_SOCKET_NAME = socketFileName("00000000", 9_999_999);

/** Seams for tests; defaults are the real environment. */
export interface SessionBusDependencyOptions {
  env?: NodeJS.ProcessEnv;
  /** PATH directories searched for git (default: env.PATH). */
  pathDirs?: string[];
  /** Process runner (git probes). */
  run?: typeof runProcess;
  /** Expected owner of the directories (default: the current uid). */
  uid?: number;
  /** Temporary directory used by the socket fallback (default: os.tmpdir()). */
  tmpdir?: () => string;
}

export interface SessionBusDoctorState {
  platform: NodeJS.Platform;
  busDir: () => string;
  allowedModes: ReadonlySet<string>;
  /** Whether this session's endpoint is running, and its id. */
  endpoint: () => { running: boolean; id?: string };
}

interface CheckContext { cwd?: string; mode?: string }

function message(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function currentUid(): number | undefined {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

function writable(dir: string): boolean {
  try {
    accessSync(dir, constants.W_OK | constants.X_OK);
    return true;
  } catch {
    return false;
  }
}

type DirInspection = { ok: true; detail: string } | { ok: false; detail: string; action: string };

/**
 * Read-only version of ensurePrivateDir: a missing dir is fine when its nearest existing ancestor is a writable
 * directory (session-bus creates it 0700 on first use); an existing one must be a real directory owned by `uid`.
 */
export function inspectPrivateDir(dir: string, uid: number | undefined, fix: string): DirInspection {
  let st;
  try {
    st = lstatSync(dir);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code !== "ENOENT") {
      return { ok: false, detail: `cannot inspect ${dir}: ${message(err)}`, action: fix };
    }
    for (let parent = dirname(dir); ; parent = dirname(parent)) {
      let pst;
      try {
        pst = statSync(parent);
      } catch (perr) {
        if ((perr as NodeJS.ErrnoException).code !== "ENOENT") {
          return { ok: false, detail: `${dir} cannot be created: cannot inspect ${parent}: ${message(perr)}`, action: fix };
        }
        if (dirname(parent) === parent) break;
        continue;
      }
      if (!pst.isDirectory()) {
        return { ok: false, detail: `${dir} cannot be created: ${parent} is not a directory`, action: fix };
      }
      if (!writable(parent)) {
        return { ok: false, detail: `${dir} does not exist and ${parent} is not writable`, action: `mkdir -p -m 700 ${dir} (as its owner), or ${fix}` };
      }
      return { ok: true, detail: `${dir} does not exist yet; created with mode 0700 on first use` };
    }
    return { ok: false, detail: `${dir} cannot be created: no existing parent directory`, action: fix };
  }
  if (st.isSymbolicLink()) return { ok: false, detail: `${dir} is a symbolic link (refused)`, action: `Remove the symlink ${dir}, or ${fix}` };
  if (!st.isDirectory()) return { ok: false, detail: `${dir} is not a directory`, action: `Move ${dir} away, or ${fix}` };
  if (uid !== undefined && st.uid !== uid) {
    return { ok: false, detail: `${dir} is owned by uid ${st.uid}, not by uid ${uid} (refused)`, action: `Use a directory you own: ${fix}` };
  }
  if (!writable(dir)) return { ok: false, detail: `${dir} is not writable`, action: `chmod 700 ${dir}` };
  const mode = st.mode & 0o777;
  const modeText = mode === 0o700 ? "mode 0700" : `mode 0${mode.toString(8)} (tightened to 0700 when session-bus starts)`;
  return { ok: true, detail: `${dir}, ${modeText}` };
}

function platformItem(platform: NodeJS.Platform): DependencyItem {
  if (platform === "win32") {
    return {
      name: "platform",
      status: "error",
      detail: "Windows is unsupported: session-bus needs POSIX Unix domain sockets",
      action: "Run Pi on Linux, macOS or inside WSL to use session-bus (nothing can be installed to fix this on Windows)",
    };
  }
  return { name: "platform", status: "ok", detail: `${platform} (POSIX, Unix domain sockets)` };
}

function busDirItems(dir: string, options: SessionBusDependencyOptions): DependencyItem[] {
  const env = options.env ?? process.env;
  const uid = options.uid ?? currentUid();
  const overridden = (env.PI_SESSION_BUS_DIR ?? "").trim().length > 0;
  const fix = "set PI_SESSION_BUS_DIR to a private directory you own (e.g. export PI_SESSION_BUS_DIR=$HOME/.pi-bus), then /reload";
  const items: DependencyItem[] = [];
  const inspected = inspectPrivateDir(dir, uid, fix);
  const source = overridden ? " (from PI_SESSION_BUS_DIR)" : "";
  items.push(inspected.ok
    ? { name: "bus directory", status: "ok", detail: `${inspected.detail}${source}` }
    : { name: "bus directory", status: "error", detail: `${inspected.detail}${source}`, action: inspected.action });

  const preferred = join(dir, WORST_SOCKET_NAME);
  const bytes = Buffer.byteLength(preferred);
  if (bytes <= MAX_SOCKET_PATH_BYTES) {
    items.push({ name: "socket path length", status: "ok", detail: `up to ${bytes} of ${MAX_SOCKET_PATH_BYTES} bytes in the bus directory` });
    return items;
  }
  const fallback = fallbackDir(dir, { env, tmpdir: options.tmpdir ?? osTmpdir, ...(uid === undefined ? {} : { uid }) });
  const fallbackBytes = Buffer.byteLength(join(fallback, WORST_SOCKET_NAME));
  const shorter = "Set PI_SESSION_BUS_DIR to a shorter path (e.g. export PI_SESSION_BUS_DIR=$HOME/.pi-bus) or XDG_RUNTIME_DIR to a short private directory, then /reload";
  if (fallbackBytes > MAX_SOCKET_PATH_BYTES) {
    items.push({
      name: "socket path length",
      status: "error",
      detail: `socket paths would exceed ${MAX_SOCKET_PATH_BYTES} bytes both in the bus directory (${bytes}) and in the fallback ${fallback} (${fallbackBytes})`,
      action: shorter,
    });
    return items;
  }
  const fallbackFix = "set XDG_RUNTIME_DIR to a private directory you own, or shorten PI_SESSION_BUS_DIR, then /reload";
  for (const level of [dirname(fallback), fallback]) {
    const result = inspectPrivateDir(level, uid, fallbackFix);
    if (!result.ok) {
      items.push({ name: "socket path length", status: "error", detail: `bus directory path is too long for sockets (${bytes} bytes); fallback: ${result.detail}`, action: result.action });
      return items;
    }
  }
  items.push({
    name: "socket path length",
    status: "ok",
    detail: `bus directory path too long for sockets (${bytes} > ${MAX_SOCKET_PATH_BYTES} bytes); sockets go to the private fallback ${fallback} (${fallbackBytes} bytes)`,
  });
  return items;
}

function receivingItem(state: SessionBusDoctorState, mode: string | undefined): DependencyItem {
  const verify = "Start pi in interactive (TUI) or RPC mode, then run /bus list: it must say \"receiving\"";
  if (mode === undefined) {
    return { name: "receiving (run mode)", status: "unverified", detail: "the run mode of this session is unknown", action: verify };
  }
  if (!state.allowedModes.has(mode)) {
    return {
      name: "receiving (run mode)",
      status: "unverified",
      detail: `this session runs in ${mode} mode: it can send but does not receive notes (receiving needs ${[...state.allowedModes].join(" or ") || "an allowed mode"})`,
      action: verify,
    };
  }
  const ep = state.endpoint();
  if (ep.running) return { name: "receiving (run mode)", status: "ok", detail: `${mode} mode, endpoint ${ep.id ?? "running"}` };
  return {
    name: "receiving (run mode)",
    status: "error",
    detail: `${mode} mode, but this session's bus endpoint is not running`,
    action: "Fix the bus directory / socket items above (see the session-bus startup warning), then /reload and check /bus list",
  };
}

async function gitItems(cwd: string | undefined, options: SessionBusDependencyOptions): Promise<DependencyItem[]> {
  const env = options.env ?? process.env;
  const run = options.run ?? runProcess;
  const git = findExecutable("git", env, options.pathDirs);
  if (!git) {
    return [{
      name: "git",
      status: "warn",
      optional: true,
      detail: "not found on PATH; the /queue repository work queue is unavailable (/bus works without it)",
      action: "Install git (e.g. sudo apt install git, sudo dnf install git, or brew install git / xcode-select --install on macOS), then /reload",
    }];
  }
  const version = await run(git, ["--version"], { env, timeoutMs: GIT_TIMEOUT_MS });
  if (version.code !== 0) {
    return [{
      name: "git",
      status: "warn",
      optional: true,
      detail: `${git} --version failed: ${outputTail(version) || `exit ${version.code}`}; /queue cannot work`,
      action: `Run ${git} --version by hand and repair or reinstall git`,
    }];
  }
  const items: DependencyItem[] = [{ name: "git", status: "ok", optional: true, detail: `${version.stdout.trim() || "git"} (${git})` }];
  if (cwd === undefined) {
    items.push({ name: "git work tree", status: "unverified", optional: true, detail: "no working directory to check", action: "Run git rev-parse --show-toplevel in the directory where you use /queue" });
    return items;
  }
  const top = await run(git, ["rev-parse", "--show-toplevel"], { cwd, env, timeoutMs: GIT_TIMEOUT_MS });
  if (top.code === 0 && top.stdout.trim()) {
    items.push({ name: "git work tree", status: "ok", optional: true, detail: `${cwd} is inside ${top.stdout.trim()} (/queue usable here)` });
  } else {
    items.push({
      name: "git work tree",
      status: "warn",
      optional: true,
      detail: `${cwd} is not inside a git work tree${top.timedOut ? " (git timed out)" : ""}; /queue is unavailable here (/bus works anywhere)`,
      action: "Use /queue from a session started inside a git repository (cd into the repository and start pi there)",
    });
  }
  return items;
}

/** Read-only check of the session-bus runtime prerequisites. */
export async function checkSessionBus(state: SessionBusDoctorState, ctx: CheckContext | undefined, options: SessionBusDependencyOptions = {}): Promise<DependencyReport> {
  const platform = platformItem(state.platform);
  if (state.platform === "win32") return { note: CHECK_NOTE, items: [platform] };
  const items: DependencyItem[] = [platform];
  let dir: string | undefined;
  try {
    dir = state.busDir();
  } catch (err) {
    items.push({ name: "bus directory", status: "error", detail: `cannot resolve the bus directory: ${message(err)}`, action: "Set PI_SESSION_BUS_DIR to a private directory you own, then /reload" });
  }
  if (dir !== undefined) items.push(...busDirItems(dir, options));
  let mode: string | undefined;
  let cwd: string | undefined;
  try { mode = ctx?.mode; } catch { /* stale context */ }
  try { cwd = ctx?.cwd; } catch { /* stale context */ }
  items.push(receivingItem(state, mode));
  items.push(...await gitItems(cwd, options));
  return { note: CHECK_NOTE, items };
}

/**
 * Nothing is installed automatically: present prerequisites are "ok" (already available), missing or broken ones
 * become "manual" with the check's action; unverified items stay unverified. Repeatable.
 */
export async function installSessionBus(state: SessionBusDoctorState, ctx: CheckContext | undefined, options: SessionBusDependencyOptions = {}): Promise<DependencyReport> {
  const report = await checkSessionBus(state, ctx, options);
  const items = report.items.map((item): DependencyItem => {
    if (item.status === "ok") return { ...item, detail: item.detail ? `already available: ${item.detail}` : "already available" };
    if (item.status === "error" || item.status === "warn" || item.status === "failed") return { ...item, status: "manual" };
    return item;
  });
  return { note: INSTALL_NOTE, items };
}

/** The provider answered on the umbrella channel and run by `/bus doctor|install-deps`; installs never overlap. */
export function createSessionBusDependencyProvider(state: SessionBusDoctorState, options: SessionBusDependencyOptions = {}): DependencyProvider<CheckContext | undefined> {
  let installing: Promise<DependencyReport> | undefined;
  return {
    id: DEPENDENCY_ID,
    label: DEPENDENCY_LABEL,
    command: DEPENDENCY_COMMAND,
    check: ctx => checkSessionBus(state, ctx, options),
    install: ctx => {
      installing ??= installSessionBus(state, ctx, options).finally(() => { installing = undefined; });
      return installing;
    },
  };
}
