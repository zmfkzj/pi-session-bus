/**
 * Bus directory, ids, socket paths and registry entries (POSIX, synchronous fs calls).
 *
 * Layout
 *   <busDir>/<id>.json   registry entry {v:1,id,sessionId,pid,socket,startedAt}, mode 0600,
 *                        written atomically (temp file + rename)
 *   <busDir>/<id>.sock   Unix socket, mode 0600 (or in the private fallback dir, see below)
 *
 * The bus dir is `$PI_SESSION_BUS_DIR` or `<getAgentDir()>/session-bus`; it is created 0700.
 * A symlink or a directory owned by another uid is refused (`BusDirError`); a directory we
 * own is tightened to 0700. When `<busDir>/<id>.sock` would exceed 103 bytes (sun_path
 * limit) the socket lives in `${XDG_RUNTIME_DIR || os.tmpdir()}/pi-session-bus-<uid>/`,
 * which gets the same checks; the registry entry always records the absolute socket path.
 *
 * Public API (for the extension wiring):
 *   resolveBusDir, ensurePrivateDir, BusDirError, deriveId, prepareSocketPath,
 *   writeEntry, readEntry, listEntries, removeEntry.
 */

import { createHash, randomBytes } from "node:crypto";
import {
  chmodSync,
  closeSync,
  constants as fsConstants,
  fchmodSync,
  fstatSync,
  lstatSync,
  mkdirSync,
  openSync,
  readdirSync,
  readFileSync,
  renameSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

/** Longest socket path we allow (sun_path is 104 on macOS, 108 on Linux; 103 + NUL fits both). */
export const MAX_SOCKET_PATH_BYTES = 103;
export const ID_PATTERN = /^[0-9a-f]{8}$/;

export interface RegistryEntry {
  v: 1;
  id: string;
  sessionId: string;
  pid: number;
  /** Absolute path of the Unix socket. */
  socket: string;
  /** ISO timestamp. */
  startedAt: string;
}

export type BusDirErrorCode = "symlink" | "not_directory" | "foreign_owner" | "io" | "path_too_long";

export class BusDirError extends Error {
  readonly code: BusDirErrorCode;
  readonly dir: string;

  constructor(code: BusDirErrorCode, dir: string, message: string) {
    super(message);
    this.name = "BusDirError";
    this.code = code;
    this.dir = dir;
  }
}

function currentUid(): number | undefined {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

export interface ResolveBusDirOptions {
  env?: Record<string, string | undefined>;
  /** Injected so this module never imports the pi runtime. */
  getAgentDir: () => string;
}

/** `$PI_SESSION_BUS_DIR` if set and non-empty, else `<getAgentDir()>/session-bus`. Always absolute. */
export function resolveBusDir(options: ResolveBusDirOptions): string {
  const env = options.env ?? process.env;
  const override = env.PI_SESSION_BUS_DIR;
  if (override !== undefined && override.trim().length > 0) return resolve(override);
  return join(options.getAgentDir(), "session-bus");
}

export interface EnsureDirOptions {
  /** Expected owner; defaults to the current uid. */
  uid?: number;
}

/**
 * Create `dir` (mode 0700, parents included) if missing and verify it: it must be a real
 * directory (not a symlink) owned by us; its mode is then forced to 0700.
 * Throws BusDirError otherwise.
 */
export function ensurePrivateDir(dir: string, options: EnsureDirOptions = {}): void {
  const uid = options.uid ?? currentUid();
  try {
    mkdirSync(dir, { recursive: true, mode: 0o700 });
  } catch (err) {
    // EEXIST: something that is not a directory sits there; the checks below name the problem.
    if ((err as NodeJS.ErrnoException).code !== "EEXIST") {
      throw new BusDirError("io", dir, `cannot create ${dir}: ${(err as Error).message}`);
    }
  }
  let st;
  try {
    st = lstatSync(dir);
  } catch (err) {
    throw new BusDirError("io", dir, `cannot inspect ${dir}: ${(err as Error).message}`);
  }
  if (st.isSymbolicLink()) throw new BusDirError("symlink", dir, `${dir} is a symbolic link; refusing to use it`);
  if (!st.isDirectory()) throw new BusDirError("not_directory", dir, `${dir} is not a directory`);
  if (uid !== undefined && st.uid !== uid) {
    throw new BusDirError("foreign_owner", dir, `${dir} is owned by uid ${st.uid}, not by uid ${uid}; refusing to use it`);
  }
  // Re-check on an O_NOFOLLOW descriptor so a swap after lstat cannot redirect the chmod.
  let fd: number | undefined;
  try {
    fd = openSync(dir, fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW);
    const fst = fstatSync(fd);
    if (uid !== undefined && fst.uid !== uid) {
      throw new BusDirError("foreign_owner", dir, `${dir} is owned by uid ${fst.uid}, not by uid ${uid}; refusing to use it`);
    }
    if ((fst.mode & 0o777) !== 0o700) fchmodSync(fd, 0o700);
  } catch (err) {
    if (err instanceof BusDirError) throw err;
    const code = (err as NodeJS.ErrnoException).code;
    if (code === "ELOOP" || code === "ENOTDIR") {
      throw new BusDirError("symlink", dir, `${dir} is a symbolic link or not a directory; refusing to use it`);
    }
    throw new BusDirError("io", dir, `cannot secure ${dir}: ${(err as Error).message}`);
  } finally {
    if (fd !== undefined) closeSync(fd);
  }
}

/**
 * 8 lowercase hex characters derived from sessionId + pid (+ salt). Stable across /reload
 * (same session, same process) yet different for two processes on the same session file.
 */
export function deriveId(sessionId: string, pid: number, salt?: string | number): string {
  const material = salt === undefined ? `${sessionId}\0${pid}` : `${sessionId}\0${pid}\0${salt}`;
  return createHash("sha256").update(material).digest("hex").slice(0, 8);
}

export interface SocketPathOptions {
  env?: Record<string, string | undefined>;
  tmpdir?: () => string;
  uid?: number;
  maxBytes?: number;
}

/**
 * Absolute socket path for `id`, creating (and checking) the fallback directory when the
 * path inside `busDir` is too long. Throws BusDirError("path_too_long") if even the fallback
 * does not fit.
 */
export function prepareSocketPath(busDir: string, id: string, options: SocketPathOptions = {}): string {
  const max = options.maxBytes ?? MAX_SOCKET_PATH_BYTES;
  const preferred = join(busDir, `${id}.sock`);
  if (Buffer.byteLength(preferred) <= max) return preferred;
  const env = options.env ?? process.env;
  const uid = options.uid ?? currentUid();
  const runtime = env.XDG_RUNTIME_DIR;
  const base = runtime !== undefined && runtime.length > 0 ? runtime : (options.tmpdir ?? tmpdir)();
  const fallbackDir = join(base, `pi-session-bus-${uid ?? "user"}`);
  const fallback = join(fallbackDir, `${id}.sock`);
  if (Buffer.byteLength(fallback) > max) {
    throw new BusDirError("path_too_long", fallbackDir, `socket path ${fallback} exceeds ${max} bytes`);
  }
  ensurePrivateDir(fallbackDir, uid === undefined ? {} : { uid });
  return fallback;
}

// ---------------------------------------------------------------------------
// Registry entries
// ---------------------------------------------------------------------------

export function entryPath(busDir: string, id: string): string {
  return join(busDir, `${id}.json`);
}

/** Atomically write `<busDir>/<id>.json` with mode 0600 (temp file in the same dir + rename). */
export function writeEntry(busDir: string, entry: RegistryEntry): void {
  const target = entryPath(busDir, entry.id);
  const tmp = join(busDir, `.${entry.id}.${process.pid}.${randomBytes(4).toString("hex")}.tmp`);
  try {
    writeFileSync(tmp, `${JSON.stringify(entry)}\n`, { mode: 0o600, flag: "wx" });
    chmodSync(tmp, 0o600);
    renameSync(tmp, target);
  } catch (err) {
    try {
      unlinkSync(tmp);
    } catch {
      /* already gone */
    }
    throw err;
  }
}

function validEntry(value: unknown, expectedId: string): RegistryEntry | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  const o = value as Record<string, unknown>;
  if (o.v !== 1 || o.id !== expectedId || !ID_PATTERN.test(expectedId)) return undefined;
  if (typeof o.sessionId !== "string" || typeof o.startedAt !== "string") return undefined;
  if (typeof o.pid !== "number" || !Number.isInteger(o.pid) || o.pid <= 0) return undefined;
  if (typeof o.socket !== "string" || !isAbsolute(o.socket)) return undefined;
  return { v: 1, id: o.id, sessionId: o.sessionId, pid: o.pid, socket: o.socket, startedAt: o.startedAt };
}

/** Read one entry; undefined when missing, unreadable, malformed or not matching its file name. */
export function readEntry(busDir: string, id: string): RegistryEntry | undefined {
  try {
    return validEntry(JSON.parse(readFileSync(entryPath(busDir, id), "utf8")), id);
  } catch {
    return undefined;
  }
}

/** All valid entries in `busDir`, oldest first (by startedAt, then id). Missing dir => []. */
export function listEntries(busDir: string): RegistryEntry[] {
  let names: string[];
  try {
    names = readdirSync(busDir);
  } catch {
    return [];
  }
  const entries: RegistryEntry[] = [];
  for (const name of names) {
    if (!name.endsWith(".json")) continue;
    const id = name.slice(0, -".json".length);
    if (!ID_PATTERN.test(id)) continue;
    const entry = readEntry(busDir, id);
    if (entry) entries.push(entry);
  }
  entries.sort((a, b) => (a.startedAt === b.startedAt ? a.id.localeCompare(b.id) : a.startedAt.localeCompare(b.startedAt)));
  return entries;
}

function unlinkQuiet(path: string): void {
  try {
    unlinkSync(path);
  } catch {
    /* missing or not removable: best effort */
  }
}

/**
 * Remove `<busDir>/<id>.json` and the socket. The recorded socket path is only unlinked
 * when its file name is `<id>.sock` (a hostile entry cannot make us delete other files);
 * `<busDir>/<id>.sock` is always attempted.
 */
export function removeEntry(busDir: string, entry: Pick<RegistryEntry, "id" | "socket">): void {
  unlinkQuiet(entryPath(busDir, entry.id));
  unlinkQuiet(join(busDir, `${entry.id}.sock`));
  if (isAbsolute(entry.socket) && basename(entry.socket) === `${entry.id}.sock` && dirname(entry.socket) !== busDir) {
    unlinkQuiet(entry.socket);
  }
}
