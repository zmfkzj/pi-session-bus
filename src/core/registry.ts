/**
 * Bus directory, ids and socket files (POSIX, synchronous fs calls). The socket directory is
 * the registry: there are no other files.
 *
 * Layout
 *   <busDir>/<id>-<pid>.sock   Unix socket, mode 0600; <id> = 8 lowercase hex chars (deriveId),
 *                              <pid> = pid of the owning process (decimal, no leading zero)
 *   ${XDG_RUNTIME_DIR || os.tmpdir()}/pi-session-bus-<uid>/<hash8>/<id>-<pid>.sock
 *                              fallback when the path inside busDir would exceed 103 bytes
 *                              (sun_path limit); <hash8> = first 8 hex chars of sha256(absolute busDir)
 *
 * The bus dir is `$PI_SESSION_BUS_DIR` or `<getAgentDir()>/session-bus`; it is created 0700.
 * A symlink or a directory owned by another uid is refused (`BusDirError`); a directory we
 * own is tightened to 0700. Both levels of the fallback path get the same checks. The hash
 * keeps buses with different bus dirs (tests, other setups) apart in the shared fallback dir.
 *
 * Discovery = `listSockets`: names must match the pattern and be real sockets (lstat), so a
 * regular file, a symlink or a foreign name is never listed (nor removed by `removeSocket`).
 * The fallback dir is only read when it is a directory (no symlink) owned by us.
 *
 * Public API (for the extension wiring):
 *   resolveBusDir, ensurePrivateDir, BusDirError, deriveId, socketFileName, parseSocketFileName,
 *   fallbackDir, prepareSocketPath, listSockets, removeSocket.
 */

import { createHash } from "node:crypto";
import {
  closeSync,
  constants as fsConstants,
  fchmodSync,
  fstatSync,
  lstatSync,
  mkdirSync,
  openSync,
  readdirSync,
  unlinkSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";

/** Longest socket path we allow (sun_path is 104 on macOS, 108 on Linux; 103 + NUL fits both). */
export const MAX_SOCKET_PATH_BYTES = 103;
export const ID_PATTERN = /^[0-9a-f]{8}$/;

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

const SOCKET_FILE_PATTERN = /^([0-9a-f]{8})-([1-9][0-9]*)\.sock$/;

/** `<id>-<pid>.sock` */
export function socketFileName(id: string, pid: number): string {
  return `${id}-${pid}.sock`;
}

/** Inverse of socketFileName; undefined for any name that does not match the pattern exactly. */
export function parseSocketFileName(name: string): { id: string; pid: number } | undefined {
  const m = SOCKET_FILE_PATTERN.exec(name);
  if (!m) return undefined;
  const pid = Number(m[2]);
  if (!Number.isSafeInteger(pid)) return undefined;
  return { id: m[1] as string, pid };
}

/**
 * Private fallback directory for `busDir`:
 * `${XDG_RUNTIME_DIR || tmpdir()}/pi-session-bus-<uid>/<first 8 hex chars of sha256(absolute busDir)>`.
 * Only computes the path; nothing is created.
 */
export function fallbackDir(busDir: string, options: SocketPathOptions = {}): string {
  const env = options.env ?? process.env;
  const uid = options.uid ?? currentUid();
  const runtime = env.XDG_RUNTIME_DIR;
  const base = runtime !== undefined && runtime.length > 0 ? runtime : (options.tmpdir ?? tmpdir)();
  const hash = createHash("sha256").update(resolve(busDir)).digest("hex").slice(0, 8);
  return join(base, `pi-session-bus-${uid ?? "user"}`, hash);
}

/**
 * Absolute socket path for `<id>-<pid>`: inside `busDir`, or in the fallback directory when
 * that path would be too long. Only in the latter case the fallback directory (both levels,
 * 0700) is created and checked with ensurePrivateDir. Throws BusDirError("path_too_long") if
 * even the fallback does not fit.
 */
export function prepareSocketPath(busDir: string, id: string, pid: number, options: SocketPathOptions = {}): string {
  const max = options.maxBytes ?? MAX_SOCKET_PATH_BYTES;
  const name = socketFileName(id, pid);
  const preferred = join(busDir, name);
  if (Buffer.byteLength(preferred) <= max) return preferred;
  const uid = options.uid ?? currentUid();
  const dir = fallbackDir(busDir, options);
  const fallback = join(dir, name);
  if (Buffer.byteLength(fallback) > max) {
    throw new BusDirError("path_too_long", dir, `socket path ${fallback} exceeds ${max} bytes`);
  }
  const owner = uid === undefined ? {} : { uid };
  ensurePrivateDir(dirname(dir), owner);
  ensurePrivateDir(dir, owner);
  return fallback;
}

/** A socket file found in the bus dir or its fallback dir. */
export interface SocketEntry {
  id: string;
  pid: number;
  /** Absolute path of the socket file. */
  path: string;
  mtimeMs: number;
}

/** Read-only check for the fallback dir: a real directory (no symlink) owned by us, at both levels. */
function isOwnDir(dir: string, uid: number | undefined): boolean {
  try {
    const st = lstatSync(dir);
    return st.isDirectory() && (uid === undefined || st.uid === uid);
  } catch {
    return false;
  }
}

function scanSockets(dir: string, out: SocketEntry[], seen: Set<string>): void {
  let names: string[];
  try {
    names = readdirSync(dir);
  } catch {
    return;
  }
  for (const name of names) {
    const parsed = parseSocketFileName(name);
    if (!parsed) continue;
    const path = join(dir, name);
    if (seen.has(path)) continue;
    try {
      const st = lstatSync(path);
      if (!st.isSocket()) continue; // regular files, symlinks, directories: not ours to list
      seen.add(path);
      out.push({ id: parsed.id, pid: parsed.pid, path, mtimeMs: st.mtimeMs });
    } catch {
      /* vanished meanwhile */
    }
  }
}

/**
 * Every socket named `<id>-<pid>.sock` in `busDir` and, if it exists (and is private to us),
 * in the fallback dir. Oldest first (mtime, then id, then pid). Missing dirs => nothing.
 */
export function listSockets(busDir: string, options: SocketPathOptions = {}): SocketEntry[] {
  const out: SocketEntry[] = [];
  const seen = new Set<string>();
  scanSockets(busDir, out, seen);
  const dir = fallbackDir(busDir, options);
  const uid = options.uid ?? currentUid();
  if (isOwnDir(dirname(dir), uid) && isOwnDir(dir, uid)) scanSockets(dir, out, seen);
  out.sort((a, b) => a.mtimeMs - b.mtimeMs || a.id.localeCompare(b.id) || a.pid - b.pid);
  return out;
}

/**
 * Unlink `path` only if lstat says it is a socket (never a regular file, symlink or directory).
 * Best effort: a vanished file (ENOENT) or any other failure is ignored. True if it was unlinked.
 */
export function removeSocket(path: string): boolean {
  try {
    if (!lstatSync(path).isSocket()) return false;
    unlinkSync(path);
    return true;
  } catch {
    return false;
  }
}
