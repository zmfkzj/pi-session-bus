import { createQueueStore } from "../../src/core/queue.ts";
const [busDir, repo, prefix, ownerPid, count = "12"] = process.argv.slice(2);
if (!busDir || !repo || !prefix || !ownerPid) throw new Error("missing worker arguments");
const store = createQueueStore({ busDir, repo, lockTimeoutMs: 20_000 });
for (let i = 0; i < Number(count); i++) {
  const result = await store.enqueue({ endpointId: `${prefix}${i.toString(16).padStart(4, "0")}`, pid: Number(ownerPid),
    sessionId: `${prefix}-${i}`, title: `task ${i}` });
  // Reported only after the update returned successfully: the parent checks that none of these is ever lost.
  process.stdout.write(`${result.entry.id}\n`);
  // Exercise contention across OS processes without making the critical section asynchronous.
  await store.mutate(() => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2); });
}
