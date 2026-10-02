/**
 * Target resolution for session_send and /bus send (pure).
 *
 * `resolveTarget(query, self, peers)` tries, in order:
 *   1. exact id
 *   2. exact sessionId
 *   3. case-insensitive exact name
 *   4. unique prefix (>= MIN_PREFIX_LENGTH chars, case-insensitive) of id or sessionId
 * At each stage the matching *peers* decide: one match is `ok`, several are `ambiguous`
 * (candidates are peers only). Only when no peer matches at a stage and `self` does is the
 * result `self`. (Two processes may share a sessionId; the other process wins over self.)
 * Nothing matching at any stage is `unknown`.
 */

export const MIN_PREFIX_LENGTH = 4;

export interface ResolvablePeer {
  id: string;
  sessionId: string;
  name?: string;
}

export type Resolution<P extends ResolvablePeer> =
  | { status: "ok"; peer: P }
  | { status: "self" }
  | { status: "unknown" }
  | { status: "ambiguous"; candidates: P[] };

type Matcher = (p: ResolvablePeer) => boolean;

export function resolveTarget<P extends ResolvablePeer>(
  query: string,
  self: ResolvablePeer,
  peers: readonly P[],
): Resolution<P> {
  const q = query.trim();
  if (q.length === 0) return { status: "unknown" };
  const others = peers.filter((p) => p.id !== self.id);
  const lower = q.toLowerCase();

  const stages: Matcher[] = [
    (p) => p.id === q,
    (p) => p.sessionId === q,
    (p) => p.name !== undefined && p.name.toLowerCase() === lower,
  ];
  if (q.length >= MIN_PREFIX_LENGTH) {
    stages.push((p) => p.id.toLowerCase().startsWith(lower) || p.sessionId.toLowerCase().startsWith(lower));
  }

  for (const matches of stages) {
    const hits = others.filter(matches);
    if (hits.length === 1) return { status: "ok", peer: hits[0]! };
    if (hits.length > 1) return { status: "ambiguous", candidates: hits };
    if (matches(self)) return { status: "self" };
  }
  return { status: "unknown" };
}
