import { createQueueStore } from "../../src/core/queue.ts";
const [busDir, repo, prefix, ownerPid] = process.argv.slice(2);
if (!busDir || !repo || !prefix || !ownerPid) throw new Error("missing worker arguments");
const store = createQueueStore({ busDir, repo });
for (let i = 0; i < 12; i++) {
  await store.enqueue({ endpointId: `${prefix}${i.toString(16).padStart(4, "0")}`, pid: Number(ownerPid),
    sessionId: `${prefix}-${i}`, title: `task ${i}` });
  // Exercise contention across OS processes without making the critical section asynchronous.
  await store.mutate(() => { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 2); });
}
