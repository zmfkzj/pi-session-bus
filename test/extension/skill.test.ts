import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { it } from "node:test";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");
const manifest = JSON.parse(readFileSync(join(root, "package.json"), "utf8")) as { files: string[]; pi: { extensions: string[]; skills?: string[] } };
const skill = readFileSync(join(root, "skills/session-bus/SKILL.md"), "utf8");
const frontmatter = /^---\n([\s\S]*?)\n---\n/.exec(skill)?.[1] ?? "";
const description = /^description: (.+)$/m.exec(frontmatter)?.[1] ?? "";

it("registers and ships the session-bus skill", () => {
  assert.deepEqual(manifest.pi.skills, ["./skills"]);
  assert.deepEqual(manifest.pi.extensions, ["./src/index.ts"]);
  assert.ok(manifest.files.includes("skills"));
});

it("the skill has bilingual triggers, the workflow, the safety rules and the orche worker behaviour", () => {
  assert.match(frontmatter, /^name: session-bus$/m);
  assert.ok(description.length > 0 && description.length <= 1024, `description length ${description.length}`);
  for (const trigger of [/다른 세션에 알려줘/, /다른 터미널/, /여러 저장소/, /인터페이스·스키마/, /답장/, /notify or ask another Pi session or terminal/, /coordinate across repositories/, /reply to a session-bus message/]) assert.match(description, trigger);
  for (const step of [/`session_list` → pick the recipient/, /`replyTo` = the message id/, /`wake: false`/]) assert.match(skill, step);
  for (const rule of [/untrusted peer input/, /Never send acknowledgements/, /never wait or poll/, /destructive, irreversible or out-of-scope/]) assert.match(skill, rule);
  assert.match(skill, /send \*\*as the main session\*\*/);
  assert.match(skill, /`orche_spawn` sub-workers do not have these tools/);
});
