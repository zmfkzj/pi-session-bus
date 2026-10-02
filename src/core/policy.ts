/**
 * Wake policy: the loop guard of the session bus (pure, injectable clock).
 *
 * Hop accounting
 *   - `resetChain()` on interactive/rpc user input: chainHops = 0.
 *   - `onDelivered(hops)` for every delivered note: chainHops = min(max(chainHops, hops), maxHops + 1).
 *   - `nextToolHops()` = chainHops + 1: hops carried by notes sent through the tool.
 *     (`/bus send` is user-originated and always sends hops 1; that is the caller's job.)
 *
 * `decide(note)` is side-effect free. A note wakes the session only if ALL hold:
 *   autoWake is on, the sender asked for a wake, note.hops <= maxHops, and fewer than
 *   maxWakesPerMinute actual wakes happened in the last wakeWindowMs.
 * Otherwise it is still delivered, with the first failing reason in the order
 *   wake_off, sender_no_wake, hop_limit, rate_limit.
 * Call `recordWake()` only when a wake is actually performed.
 */

import type { WakeSuppressReason } from "./protocol.ts";

export const MAX_HOPS = 4;
export const MAX_WAKES_PER_MINUTE = 6;
export const WAKE_WINDOW_MS = 60_000;

export type WakeDecision = { wake: true } | { wake: false; reason: WakeSuppressReason };

export interface WakePolicyOptions {
  maxHops?: number;
  maxWakesPerMinute?: number;
  wakeWindowMs?: number;
  /** Initial autoWake value (default true). */
  autoWake?: boolean;
  /** Injectable clock, milliseconds. */
  now?: () => number;
}

export class WakePolicy {
  readonly maxHops: number;
  readonly maxWakesPerMinute: number;
  readonly wakeWindowMs: number;
  #autoWake: boolean;
  #chainHops = 0;
  #wakes: number[] = [];
  readonly #now: () => number;

  constructor(options: WakePolicyOptions = {}) {
    this.maxHops = options.maxHops ?? MAX_HOPS;
    this.maxWakesPerMinute = options.maxWakesPerMinute ?? MAX_WAKES_PER_MINUTE;
    this.wakeWindowMs = options.wakeWindowMs ?? WAKE_WINDOW_MS;
    this.#autoWake = options.autoWake ?? true;
    this.#now = options.now ?? Date.now;
  }

  get autoWake(): boolean {
    return this.#autoWake;
  }

  set autoWake(value: boolean) {
    this.#autoWake = value;
  }

  get chainHops(): number {
    return this.#chainHops;
  }

  /** Interactive/rpc user input starts a new conversation chain. */
  resetChain(): void {
    this.#chainHops = 0;
  }

  /**
   * Account for a delivered note (whether or not it woke the session). The chain is clamped to
   * `maxHops + 1`: a chain that passed the limit still yields outgoing hops > maxHops (suppressed at
   * the recipient), but a hostile peer's huge `hops` cannot push `nextToolHops()` out of the wire range.
   */
  onDelivered(hops: number): void {
    if (Number.isFinite(hops)) this.#chainHops = Math.min(Math.max(this.#chainHops, hops), this.maxHops + 1);
  }

  /** Hops for a note sent by the model through the tool. */
  nextToolHops(): number {
    return this.#chainHops + 1;
  }

  decide(note: { hops: number; wake: boolean }): WakeDecision {
    if (!this.#autoWake) return { wake: false, reason: "wake_off" };
    if (!note.wake) return { wake: false, reason: "sender_no_wake" };
    if (note.hops > this.maxHops) return { wake: false, reason: "hop_limit" };
    if (this.recentWakes() >= this.maxWakesPerMinute) return { wake: false, reason: "rate_limit" };
    return { wake: true };
  }

  /** Record that a wake was actually performed now. */
  recordWake(): void {
    this.#prune();
    this.#wakes.push(this.#now());
  }

  /** Number of actual wakes inside the sliding window. */
  recentWakes(): number {
    this.#prune();
    return this.#wakes.length;
  }

  #prune(): void {
    const cutoff = this.#now() - this.wakeWindowMs;
    // A wake at exactly `cutoff` is `wakeWindowMs` old and no longer counts.
    let drop = 0;
    while (drop < this.#wakes.length && this.#wakes[drop]! <= cutoff) drop++;
    if (drop > 0) this.#wakes.splice(0, drop);
  }
}
