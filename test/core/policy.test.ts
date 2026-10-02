import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { MAX_HOPS, MAX_WAKES_PER_MINUTE, WAKE_WINDOW_MS, WakePolicy } from "../../src/core/policy.ts";

function makePolicy(options: ConstructorParameters<typeof WakePolicy>[0] = {}) {
  const clock = { t: 1_000_000 };
  const policy = new WakePolicy({ now: () => clock.t, ...options });
  return { policy, clock };
}

describe("WakePolicy defaults", () => {
  it("exports the spec constants", () => {
    assert.equal(MAX_HOPS, 4);
    assert.equal(MAX_WAKES_PER_MINUTE, 6);
    assert.equal(WAKE_WINDOW_MS, 60_000);
    const { policy } = makePolicy();
    assert.equal(policy.maxHops, 4);
    assert.equal(policy.maxWakesPerMinute, 6);
    assert.equal(policy.wakeWindowMs, 60_000);
    assert.equal(policy.autoWake, true);
    assert.equal(policy.chainHops, 0);
  });
});

describe("WakePolicy.decide", () => {
  it("wakes when everything allows it", () => {
    const { policy } = makePolicy();
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true });
    assert.deepEqual(policy.decide({ hops: 4, wake: true }), { wake: true });
  });

  it("wake_off when the session's autoWake is off", () => {
    const { policy } = makePolicy();
    policy.autoWake = false;
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: false, reason: "wake_off" });
    policy.autoWake = true;
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true });
  });

  it("sender_no_wake when the sender did not ask for a wake", () => {
    const { policy } = makePolicy();
    assert.deepEqual(policy.decide({ hops: 1, wake: false }), { wake: false, reason: "sender_no_wake" });
  });

  it("hop_limit when hops exceed maxHops", () => {
    const { policy } = makePolicy();
    assert.deepEqual(policy.decide({ hops: 5, wake: true }), { wake: false, reason: "hop_limit" });
    const small = makePolicy({ maxHops: 2 }).policy;
    assert.deepEqual(small.decide({ hops: 2, wake: true }), { wake: true });
    assert.deepEqual(small.decide({ hops: 3, wake: true }), { wake: false, reason: "hop_limit" });
  });

  it("rate_limit after maxWakesPerMinute actual wakes in the window", () => {
    const { policy } = makePolicy();
    for (let i = 0; i < MAX_WAKES_PER_MINUTE; i++) {
      assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true }, `wake ${i}`);
      policy.recordWake();
    }
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: false, reason: "rate_limit" });
  });

  it("reports the first failing reason: wake_off > sender_no_wake > hop_limit > rate_limit", () => {
    const { policy } = makePolicy({ maxWakesPerMinute: 0 });
    assert.deepEqual(policy.decide({ hops: 9, wake: false }), { wake: false, reason: "sender_no_wake" });
    assert.deepEqual(policy.decide({ hops: 9, wake: true }), { wake: false, reason: "hop_limit" });
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: false, reason: "rate_limit" });
    policy.autoWake = false;
    assert.deepEqual(policy.decide({ hops: 9, wake: false }), { wake: false, reason: "wake_off" });
  });

  it("does not record anything by itself", () => {
    const { policy } = makePolicy();
    for (let i = 0; i < 20; i++) policy.decide({ hops: 1, wake: true });
    assert.equal(policy.recentWakes(), 0);
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true });
  });
});

describe("WakePolicy rate window", () => {
  it("counts only recorded wakes inside the sliding window", () => {
    const { policy, clock } = makePolicy({ maxWakesPerMinute: 3, wakeWindowMs: 1000 });
    policy.recordWake(); // t=0
    clock.t += 400;
    policy.recordWake(); // t=400
    clock.t += 400;
    policy.recordWake(); // t=800
    assert.equal(policy.recentWakes(), 3);
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: false, reason: "rate_limit" });

    clock.t += 199; // t=999: first wake is 999 ms old, still inside
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: false, reason: "rate_limit" });
    clock.t += 1; // t=1000: first wake is exactly one window old and drops out
    assert.equal(policy.recentWakes(), 2);
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true });

    clock.t += 1000; // everything expired
    assert.equal(policy.recentWakes(), 0);
  });

  it("slides: each expiring wake frees one slot", () => {
    const { policy, clock } = makePolicy({ maxWakesPerMinute: 2, wakeWindowMs: 100 });
    policy.recordWake();
    clock.t += 50;
    policy.recordWake();
    assert.equal(policy.decide({ hops: 1, wake: true }).wake, false);
    clock.t += 50; // first expires
    assert.equal(policy.decide({ hops: 1, wake: true }).wake, true);
    policy.recordWake();
    assert.equal(policy.decide({ hops: 1, wake: true }).wake, false);
    clock.t += 50; // second expires
    assert.equal(policy.decide({ hops: 1, wake: true }).wake, true);
  });

  it("suppressed notes do not consume the budget", () => {
    const { policy } = makePolicy({ maxWakesPerMinute: 1 });
    policy.autoWake = false;
    for (let i = 0; i < 5; i++) policy.decide({ hops: 1, wake: true });
    policy.autoWake = true;
    assert.deepEqual(policy.decide({ hops: 1, wake: true }), { wake: true });
  });
});

describe("WakePolicy hop accounting", () => {
  it("nextToolHops is chainHops + 1 and chainHops tracks the max delivered hop", () => {
    const { policy } = makePolicy();
    assert.equal(policy.nextToolHops(), 1);
    policy.onDelivered(1);
    assert.equal(policy.chainHops, 1);
    assert.equal(policy.nextToolHops(), 2);
    policy.onDelivered(3);
    assert.equal(policy.nextToolHops(), 4);
    policy.onDelivered(2); // lower hops never lower the chain
    assert.equal(policy.chainHops, 3);
    assert.equal(policy.nextToolHops(), 4);
  });

  it("resetChain (interactive/rpc input) starts over", () => {
    const { policy } = makePolicy();
    policy.onDelivered(4);
    assert.equal(policy.nextToolHops(), 5);
    policy.resetChain();
    assert.equal(policy.chainHops, 0);
    assert.equal(policy.nextToolHops(), 1);
  });

  it("a reply chain stops waking once hops pass maxHops", () => {
    const { policy } = makePolicy({ maxHops: 3 });
    let hops = 1; // first message from a user-originated /bus send
    const results: boolean[] = [];
    for (let i = 0; i < 5; i++) {
      const decision = policy.decide({ hops, wake: true });
      results.push(decision.wake);
      policy.onDelivered(hops);
      hops = policy.nextToolHops(); // the reply the model would send
    }
    assert.deepEqual(results, [true, true, true, false, false]);
  });

  it("clamps the chain to maxHops + 1 so a huge peer hop count stays suppressing and in the wire range", () => {
    const WIRE_MAX = 1_000_000; // largest hops value a peer accepts
    const { policy } = makePolicy();
    policy.onDelivered(WIRE_MAX);
    assert.equal(policy.chainHops, MAX_HOPS + 1);
    assert.equal(policy.nextToolHops(), MAX_HOPS + 2);
    assert.ok(policy.nextToolHops() <= WIRE_MAX);
    // a recipient with the default policy still suppresses the next outgoing note
    const recipient = makePolicy().policy;
    assert.deepEqual(recipient.decide({ hops: policy.nextToolHops(), wake: true }), { wake: false, reason: "hop_limit" });
    // further huge or lower values change nothing; user input still resets the chain
    policy.onDelivered(WIRE_MAX);
    policy.onDelivered(1);
    assert.equal(policy.nextToolHops(), MAX_HOPS + 2);
    policy.resetChain();
    assert.equal(policy.nextToolHops(), 1);

    const small = makePolicy({ maxHops: 2 }).policy;
    small.onDelivered(WIRE_MAX);
    assert.equal(small.chainHops, 3);
    assert.equal(small.nextToolHops(), 4);
  });

  it("keeps the existing accounting for values up to maxHops + 1", () => {
    const { policy } = makePolicy();
    policy.onDelivered(MAX_HOPS);
    assert.equal(policy.chainHops, MAX_HOPS);
    assert.equal(policy.nextToolHops(), MAX_HOPS + 1);
    policy.onDelivered(MAX_HOPS + 1); // exactly at the clamp: unchanged
    assert.equal(policy.chainHops, MAX_HOPS + 1);
    assert.equal(policy.nextToolHops(), MAX_HOPS + 2);
    policy.onDelivered(MAX_HOPS + 2); // beyond the clamp: capped
    assert.equal(policy.chainHops, MAX_HOPS + 1);
    policy.resetChain();
    policy.onDelivered(Number.NaN); // non-finite hops are ignored
    policy.onDelivered(Number.POSITIVE_INFINITY);
    assert.equal(policy.chainHops, 0);
  });
});
