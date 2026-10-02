import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { resolveTarget, type ResolvablePeer } from "../../src/core/resolve.ts";

const self: ResolvablePeer = { id: "00000000", sessionId: "self-session-0000", name: "me" };

function peer(id: string, sessionId: string, name?: string): ResolvablePeer {
  return name === undefined ? { id, sessionId } : { id, sessionId, name };
}

const alpha = peer("a1b2c3d4", "0190aaaa-1111-7000-8000-000000000001", "Alpha");
const beta = peer("b2c3d4e5", "0190bbbb-2222-7000-8000-000000000002", "beta");
const gamma = peer("a1ffffff", "0190aaaa-3333-7000-8000-000000000003");
const peers = [alpha, beta, gamma];

describe("resolveTarget", () => {
  it("resolves an exact id", () => {
    assert.deepEqual(resolveTarget("b2c3d4e5", self, peers), { status: "ok", peer: beta });
  });

  it("resolves an exact sessionId", () => {
    assert.deepEqual(resolveTarget("0190bbbb-2222-7000-8000-000000000002", self, peers), { status: "ok", peer: beta });
  });

  it("resolves a case-insensitive exact name", () => {
    assert.deepEqual(resolveTarget("alpha", self, peers), { status: "ok", peer: alpha });
    assert.deepEqual(resolveTarget("ALPHA", self, peers), { status: "ok", peer: alpha });
    assert.deepEqual(resolveTarget("  Beta ", self, peers), { status: "ok", peer: beta });
  });

  it("resolves a unique prefix of at least 4 characters of id or sessionId", () => {
    assert.deepEqual(resolveTarget("b2c3", self, peers), { status: "ok", peer: beta });
    assert.deepEqual(resolveTarget("0190bbbb", self, peers), { status: "ok", peer: beta });
    assert.deepEqual(resolveTarget("A1B2", self, peers), { status: "ok", peer: alpha });
  });

  it("does not use prefixes shorter than 4 characters", () => {
    assert.deepEqual(resolveTarget("b2c", self, peers), { status: "unknown" });
    assert.deepEqual(resolveTarget("a1", self, peers), { status: "unknown" });
  });

  it("reports an ambiguous prefix with its candidates", () => {
    // shared sessionId prefix
    const bySession = resolveTarget("0190aaaa", self, peers);
    assert.equal(bySession.status, "ambiguous");
    if (bySession.status === "ambiguous") assert.deepEqual(bySession.candidates, [alpha, gamma]);
    // shared id prefix
    const sibling = peer("a1b2ffff", "zzzz-0000");
    const byId = resolveTarget("a1b2", self, [alpha, sibling, beta]);
    assert.equal(byId.status, "ambiguous");
    if (byId.status === "ambiguous") assert.deepEqual(byId.candidates, [alpha, sibling]);
    // a longer prefix disambiguates
    assert.deepEqual(resolveTarget("a1ff", self, peers), { status: "ok", peer: gamma });
  });

  it("reports an ambiguous name", () => {
    const twin = peer("c3d4e5f6", "0190cccc", "alpha");
    const res = resolveTarget("alpha", self, [alpha, twin]);
    assert.equal(res.status, "ambiguous");
    if (res.status === "ambiguous") assert.deepEqual(res.candidates, [alpha, twin]);
  });

  it("prefers earlier stages: an exact id wins over a name or prefix that also matches", () => {
    const named = peer("deadbeef", "sess-x", "a1b2c3d4");
    assert.deepEqual(resolveTarget("a1b2c3d4", self, [named, alpha]), { status: "ok", peer: alpha });
    // exact name beats prefix
    const namedPrefix = peer("11112222", "sess-y", "b2c3");
    assert.deepEqual(resolveTarget("b2c3", self, [namedPrefix, beta]), { status: "ok", peer: namedPrefix });
  });

  it("returns self for our own id, sessionId, name or prefix", () => {
    assert.deepEqual(resolveTarget("00000000", self, peers), { status: "self" });
    assert.deepEqual(resolveTarget("self-session-0000", self, peers), { status: "self" });
    assert.deepEqual(resolveTarget("ME", self, peers), { status: "self" });
    assert.deepEqual(resolveTarget("self-ses", self, peers), { status: "self" });
  });

  it("prefers another process over self when both share a sessionId", () => {
    const twinProcess = peer("12345678", self.sessionId, "other-name");
    assert.deepEqual(resolveTarget(self.sessionId, self, [twinProcess]), { status: "ok", peer: twinProcess });
  });

  it("ignores a peer entry carrying our own id", () => {
    assert.deepEqual(resolveTarget("00000000", self, [{ ...self }, alpha]), { status: "self" });
  });

  it("returns unknown for unmatched or empty queries", () => {
    assert.deepEqual(resolveTarget("nobody", self, peers), { status: "unknown" });
    assert.deepEqual(resolveTarget("", self, peers), { status: "unknown" });
    assert.deepEqual(resolveTarget("   ", self, peers), { status: "unknown" });
    assert.deepEqual(resolveTarget("anything", self, []), { status: "unknown" });
  });
});
