// Dependency install / health-check contract of the oh-my-pi-extensions umbrella (`/oh-my-pi doctor|install-deps`).
//
// Each package keeps its own copy of this file (as with orche's worker-capability contract): the umbrella emits a
// request on `pi.events` channel DEPENDENCY_CHANNEL and every loaded package answers synchronously through `provide`
// with a DependencyProvider. The package's own command (`/<command> doctor` and `/<command> install-deps`) runs the
// same provider. Keep the shapes and the status vocabulary in sync with the umbrella's
// extensions/dependencies/contract.js.
//
// Rules for providers:
// - check() only inspects: it never installs, writes configuration, starts services or repairs anything.
// - install() installs only what can be installed safely without privileges, credentials or interactive authorization,
//   is safe to repeat (already present => "ok"), and reports "manual" with the exact action for everything else
//   (sudo/system packages, credentials, logins, unsupported platforms, external services).
// - every item whose status is "warn", "error", "failed" or "manual" carries an `action` (a concrete command or
//   configuration step); "unverified" items say in `action` how to verify by hand.
// - never put secrets (API keys, tokens) in `detail` or `action`; say "set" / "not set" instead.
import { spawn } from "node:child_process";
import { accessSync, constants, existsSync, readFileSync, statSync } from "node:fs";
import { delimiter, dirname, join, resolve, sep } from "node:path";

export const DEPENDENCY_CHANNEL = "oh-my-pi-extensions:dependencies";
export const DEPENDENCY_CONTRACT_VERSION = 1;

/**
 * ok: present and working (check) / already available (install). installed: installed by this install() call.
 * warn: an optional feature's dependency is missing or misconfigured (the package works without it).
 * error: a required dependency is missing or broken. failed: an install attempt failed.
 * manual: needs the user (privileges, credentials, interactive login, unsupported platform, external service).
 * unverified: could not be checked here (state it and how to check by hand).
 */
export type DependencyStatus = "ok" | "installed" | "warn" | "error" | "failed" | "manual" | "unverified";
export interface DependencyItem {
  /** What is checked, e.g. "obscura binary", "git", "CLIProxyAPI server". */
  name: string;
  status: DependencyStatus;
  /** What was found (path, version, reason). No secrets. */
  detail?: string;
  /** Remediation: required for warn, error, failed and manual; how to verify for unverified. */
  action?: string;
  /** Needed only by an optional feature. */
  optional?: boolean;
}
export interface DependencyReport {
  items: DependencyItem[];
  /** One line shown under the package heading, e.g. "No external dependencies: uses only Pi and Node.js." */
  note?: string;
}
export type DependencyMode = "check" | "install";
export interface DependencyProvider<Context = any> {
  /** Stable id: orche, browser, computer-use, pi-gui, pi-commit, session-bus, images, pi-workbench, codegraph, cliproxyapi. */
  id: string;
  /** Package name shown to the user, e.g. "pi-browser". */
  label: string;
  /** The package's own slash command, e.g. "/browser" (its subcommands are `doctor` and `install-deps`). */
  command: string;
  /** Read-only health check. Receives the command context (cwd, ui, isProjectTrusted, ...). */
  check(ctx: Context): Promise<DependencyReport>;
  /** Install what can be installed; repeatable. */
  install(ctx: Context): Promise<DependencyReport>;
}
export interface DependencyRequest {
  version: number;
  provide(provider: DependencyProvider): void;
}

export function isDependencyRequest(value: unknown): value is DependencyRequest {
  const request = value as Partial<DependencyRequest> | undefined;
  return !!request && typeof request.provide === "function";
}

/** Answer the umbrella's requests with `provider`. Returns the unsubscribe function. */
export function registerDependencyProvider(
  events: { on(channel: string, handler: (data: unknown) => void): () => void },
  provider: DependencyProvider,
): () => void {
  return events.on(DEPENDENCY_CHANNEL, data => { if (isDependencyRequest(data)) data.provide(provider); });
}

/** The package's overall outcome for one report. */
export function dependencyOutcome(report: DependencyReport, mode: DependencyMode): string {
  const statuses = new Set(report.items.map(item => item.status));
  if (report.items.length === 0) return "no external dependencies";
  if (statuses.has("error") || statuses.has("failed")) return mode === "check" ? "abnormal" : "failed";
  if (statuses.has("manual")) return "manual action required";
  if (statuses.has("warn")) return "warning";
  if (statuses.has("unverified")) return "unverified";
  if (mode === "install") return statuses.has("installed") ? "installed" : "already available";
  return "healthy";
}

export function dependencyLevel(report: DependencyReport): "info" | "warning" | "error" {
  const statuses = new Set(report.items.map(item => item.status));
  if (statuses.has("error") || statuses.has("failed")) return "error";
  if (statuses.has("manual") || statuses.has("warn") || statuses.has("unverified")) return "warning";
  return "info";
}

/** Item lines, indented by `indent`. */
export function formatDependencyItems(report: DependencyReport, indent = "  "): string[] {
  const lines: string[] = [];
  if (report.note) lines.push(`${indent}${report.note}`);
  if (report.items.length === 0 && !report.note) lines.push(`${indent}No external dependencies.`);
  for (const item of report.items) {
    lines.push(`${indent}[${item.status}] ${item.name}${item.optional ? " (optional)" : ""}${item.detail ? `: ${item.detail}` : ""}`);
    if (item.action) lines.push(`${indent}    -> ${item.action}`);
  }
  return lines;
}

/** The text and notify level of a package's own `doctor` / `install-deps` command. */
export function formatDependencyReport(label: string, report: DependencyReport, mode: DependencyMode): { text: string; level: "info" | "warning" | "error" } {
  const heading = `${label} ${mode === "check" ? "doctor" : "install-deps"}: ${dependencyOutcome(report, mode)}`;
  return { text: [heading, ...formatDependencyItems(report)].join("\n"), level: dependencyLevel(report) };
}

// ---------------------------------------------------------------------------------------------------------------
// Helpers (node builtins only).

/** First executable `name` on PATH (PATHEXT on Windows), or undefined. */
export function findExecutable(name: string, env: NodeJS.ProcessEnv = process.env, pathDirs?: string[]): string | undefined {
  const dirs = pathDirs ?? (env.PATH ?? env.Path ?? "").split(delimiter).filter(Boolean);
  const exts = process.platform === "win32" ? ["", ...(env.PATHEXT ?? ".EXE;.CMD;.BAT;.COM").split(";").filter(Boolean)] : [""];
  for (const dir of dirs) {
    for (const ext of exts) {
      const path = join(dir, name + ext);
      try {
        if (!statSync(path).isFile()) continue;
        if (process.platform !== "win32") accessSync(path, constants.X_OK);
        return path;
      } catch { /* next */ }
    }
  }
  return undefined;
}

export interface RunResult { code: number | null; stdout: string; stderr: string; error?: string; timedOut?: boolean }

/** Run without a shell (Windows .cmd shims need `shell: true`, which callers opt into). Never throws. */
export function runProcess(command: string, args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv; timeoutMs?: number; signal?: AbortSignal; shell?: boolean } = {}): Promise<RunResult> {
  return new Promise(resolvePromise => {
    let stdout = "";
    let stderr = "";
    let settled = false;
    let timedOut = false;
    const done = (result: RunResult) => { if (!settled) { settled = true; clearTimeout(timer); resolvePromise(result); } };
    let child: ReturnType<typeof spawn>;
    try {
      child = spawn(command, args, { cwd: options.cwd, env: options.env ?? process.env, shell: options.shell ?? false, signal: options.signal, stdio: ["ignore", "pipe", "pipe"], windowsHide: true });
    } catch (error) {
      resolvePromise({ code: null, stdout, stderr, error: error instanceof Error ? error.message : String(error) });
      return;
    }
    const timer = setTimeout(() => { timedOut = true; child.kill("SIGTERM"); setTimeout(() => child.kill("SIGKILL"), 2_000).unref(); }, options.timeoutMs ?? 30_000);
    timer.unref?.();
    child.stdout?.on("data", chunk => { if (stdout.length < 1_000_000) stdout += chunk; });
    child.stderr?.on("data", chunk => { if (stderr.length < 1_000_000) stderr += chunk; });
    child.on("error", error => done({ code: null, stdout, stderr, error: error.message, timedOut }));
    child.on("close", code => done({ code, stdout, stderr, timedOut, ...(timedOut ? { error: `timed out after ${options.timeoutMs ?? 30_000} ms` } : {}) }));
  });
}

/** Last `lines` non-empty lines of a process's output, for failure details. */
export function outputTail(result: RunResult, lines = 5): string {
  const text = `${result.stderr}\n${result.stdout}`.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
  return [result.error, ...text.slice(-lines)].filter(Boolean).join(" | ");
}

/** Whether `name` resolves from `packageDir` the way Node does (its node_modules or a parent's). */
export function npmPackageInstalled(packageDir: string, name: string): boolean {
  for (let dir = resolve(packageDir); ; dir = dirname(dir)) {
    if (existsSync(join(dir, "node_modules", name, "package.json"))) return true;
    if (dirname(dir) === dir) return false;
  }
}

function manifest(packageDir: string): { dependencies?: Record<string, string>; optionalDependencies?: Record<string, string> } {
  return JSON.parse(readFileSync(join(packageDir, "package.json"), "utf8"));
}

/** Missing runtime npm dependencies (package.json `dependencies`; `optionalDependencies` separately). */
export function missingNpmDependencies(packageDir: string): { required: string[]; optional: string[] } {
  const pkg = manifest(packageDir);
  const optionalNames = Object.keys(pkg.optionalDependencies ?? {});
  const required = Object.keys(pkg.dependencies ?? {}).filter(name => !optionalNames.includes(name));
  return {
    required: required.filter(name => !npmPackageInstalled(packageDir, name)),
    optional: optionalNames.filter(name => !npmPackageInstalled(packageDir, name)),
  };
}

/** Whether the package lives inside a node_modules tree (installed with `pi install npm:...`): npm must not run inside it. */
export function insideNodeModules(packageDir: string): boolean {
  return resolve(packageDir).split(sep).includes("node_modules");
}

/**
 * The check item of a package's own npm dependencies. A package whose required dependencies are missing usually does
 * not load at all (the umbrella reports that); this item still covers lazily imported and optional ones.
 * `command` is the package command ("/browser"); `optionalPurpose` names what a missing optional dependency disables.
 */
export function npmDependencyItem(packageDir: string, command: string, optionalPurpose?: Record<string, string>): DependencyItem[] {
  const pkg = manifest(packageDir);
  const total = Object.keys(pkg.dependencies ?? {}).length + Object.keys(pkg.optionalDependencies ?? {}).length;
  if (total === 0) return [];
  const { required, optional } = missingNpmDependencies(packageDir);
  const items: DependencyItem[] = [];
  const action = `Run ${command} install-deps (runs npm install --legacy-peer-deps in ${packageDir}), then /reload`;
  if (required.length) items.push({ name: "npm dependencies", status: "error", detail: `missing ${required.join(", ")}`, action });
  else items.push({ name: "npm dependencies", status: "ok", detail: `${Object.keys(pkg.dependencies ?? {}).filter(name => !(name in (pkg.optionalDependencies ?? {}))).length} installed` });
  for (const name of optional) {
    items.push({ name: `npm optional dependency ${name}`, status: "warn", optional: true, detail: `not installed${optionalPurpose?.[name] ? `: ${optionalPurpose[name]}` : ""}`, action });
  }
  return items;
}

/**
 * Install the package's missing npm dependencies with `npm install --legacy-peer-deps` (what the umbrella's bootstrap
 * runs). Already complete: "ok". Inside node_modules (an npm-installed package) or without npm: "manual".
 */
export async function installNpmDependencies(packageDir: string, options: { env?: NodeJS.ProcessEnv; signal?: AbortSignal; run?: typeof runProcess; reinstallHint?: string } = {}): Promise<DependencyItem[]> {
  const pkg = manifest(packageDir);
  if (Object.keys(pkg.dependencies ?? {}).length + Object.keys(pkg.optionalDependencies ?? {}).length === 0) return [];
  const before = missingNpmDependencies(packageDir);
  const missing = [...before.required, ...before.optional];
  if (missing.length === 0) return [{ name: "npm dependencies", status: "ok", detail: "already installed" }];
  if (insideNodeModules(packageDir)) {
    return [{ name: "npm dependencies", status: "manual", detail: `missing ${missing.join(", ")} in an npm-installed package`, action: options.reinstallHint ?? "Reinstall the package with pi install (or pi update --extensions), then /reload" }];
  }
  const windows = process.platform === "win32";
  const npm = windows ? "npm.cmd" : "npm";
  if (!findExecutable("npm", options.env)) {
    return [{ name: "npm dependencies", status: "manual", detail: `missing ${missing.join(", ")}; npm is not on PATH`, action: `Install Node.js with npm, then run: cd ${packageDir} && npm install --legacy-peer-deps` }];
  }
  const result = await (options.run ?? runProcess)(npm, ["install", "--legacy-peer-deps", "--no-audit", "--no-fund"], { cwd: packageDir, env: options.env, signal: options.signal, timeoutMs: 10 * 60_000, shell: windows });
  const after = missingNpmDependencies(packageDir);
  if (result.code !== 0 || after.required.length > 0) {
    return [{ name: "npm dependencies", status: "failed", detail: `npm install exited ${result.code ?? "abnormally"}${after.required.length ? `; still missing ${after.required.join(", ")}` : ""}: ${outputTail(result)}`, action: `Run by hand and read the error: cd ${packageDir} && npm install --legacy-peer-deps` }];
  }
  const items: DependencyItem[] = [{ name: "npm dependencies", status: "installed", detail: `installed ${missing.filter(name => !after.optional.includes(name)).join(", ") || "(lockfile refreshed)"}`, action: "Run /reload to load them" }];
  for (const name of after.optional) items.push({ name: `npm optional dependency ${name}`, status: "warn", optional: true, detail: "npm skipped it (unsupported platform or failed optional install)", action: `See npm's output: cd ${packageDir} && npm install --legacy-peer-deps` });
  return items;
}
