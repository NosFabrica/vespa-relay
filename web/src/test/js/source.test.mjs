// Every source file the page ships is text: one NUL byte in the first 8000 makes git treat a
// module as binary, with no diffs and no grep hits. Write the character as the escape `\u0000`.

import assert from "assert";
import { readdirSync, readFileSync, statSync } from "node:fs";

const root = new URL("../../main/resources/", import.meta.url);

/** Every file the page ships, module tree and markup alike. */
function sources(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const at = new URL(name, dir);
    // The trailing slash goes on only once the entry is known to be a directory.
    if (statSync(at).isDirectory()) { sources(new URL(name + "/", dir), out); continue; }
    if (/\.(js|mjs|html|css)$/.test(name)) out.push(at);
  }
  return out;
}

const files = sources(root);
assert.ok(files.length >= 20, `expected the whole web tree, found ${files.length} files`);

for (const file of files) {
  const where = decodeURIComponent(file.pathname).split("/resources/")[1];
  const bytes = readFileSync(file);

  // Rejected at any offset, not only within git's 8000-byte sniff: the next edit can move it.
  assert.strictEqual(
    bytes.indexOf(0), -1,
    `${where} contains a NUL byte at offset ${bytes.indexOf(0)} — git will call this file binary and ` +
    `stop diffing it. Write the character as the escape \\u0000 instead.`,
  );

  assert.doesNotThrow(
    () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    `${where} is not valid UTF-8`,
  );
}

// A sweep that silently covers nothing would still pass.
const field = files.find((f) => f.pathname.endsWith("/web/searchfield.js"));
assert.ok(field, "searchfield.js is not in the swept tree");

console.log(`source: ${files.length} files are text — no NUL bytes, valid UTF-8`);
