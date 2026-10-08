/**
 * Core of the session bus: no pi runtime calls, everything injectable.
 *
 *   protocol  constants, wire types, FrameDecoder/encodeFrame, parseRequest/parse*Response,
 *             createNote, rejected, formatNoteText (model-facing framed text)
 *   policy    WakePolicy (+ MAX_HOPS, MAX_WAKES_PER_MINUTE, WAKE_WINDOW_MS)
 *   resolve   resolveTarget(query, self, peers) -> ok | self | unknown | ambiguous
 *   registry  resolveBusDir, ensurePrivateDir (BusDirError), deriveId, socketFileName/parseSocketFileName,
 *             fallbackDir, prepareSocketPath, listSockets (SocketEntry), removeSocket (the socket dir is the registry)
 *   endpoint  createEndpoint (server) and helloProbe/sendNote/listPeers (client, BusClientError)
 */

export * from "./endpoint.ts";
export * from "./policy.ts";
export * from "./protocol.ts";
export * from "./registry.ts";
export * from "./resolve.ts";
export * from "./queue.ts";
export { QueueFencedError, STALE_LOCK_MS } from "./queue-log.ts";
