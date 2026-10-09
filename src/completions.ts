/**
 * Argument completions for `/bus` and `/queue`. Pi passes everything after the command name and replaces it with the chosen value, so
 * each candidate is a whole argument string the handler accepts. Free text (`/bus send <to> <text>`, a `/queue <prompt>`) is never
 * completed; `send ` ends with a space so the peer can be typed right away.
 */
export interface CommandCompletion { value: string; label: string; description?: string }

export const BUS_COMPLETIONS: readonly CommandCompletion[] = [
  { value: "list", label: "list", description: "List live sessions on the bus" },
  { value: "send ", label: "send <to> <text>", description: "Send a note to another session and wake it" },
  { value: "wake on", label: "wake on", description: "Let incoming notes wake this session" },
  { value: "wake off", label: "wake off", description: "Do not let incoming notes wake this session" },
];

export const QUEUE_COMPLETIONS: readonly CommandCompletion[] = [
  { value: "list", label: "list", description: "Show the repository work queue" },
  { value: "on", label: "on", description: "Turn repository queue mode on" },
  { value: "off", label: "off", description: "Turn repository queue mode off" },
  { value: "done", label: "done", description: "Hand over this session's repository turn" },
];

/** Candidates starting with the typed text; a lone exact match is dropped so Enter submits the command instead of re-applying it. */
export function completeFrom(candidates: readonly CommandCompletion[], prefix: string): CommandCompletion[] | null {
  const typed = prefix.trimStart();
  const items = candidates.filter(item => item.value.startsWith(typed));
  if (items.length === 0 || (items.length === 1 && items[0]!.value === typed)) return null;
  return items.map(item => ({ ...item }));
}

export const completeBusArguments = (prefix: string) => completeFrom(BUS_COMPLETIONS, prefix);
export const completeQueueArguments = (prefix: string) => completeFrom(QUEUE_COMPLETIONS, prefix);
