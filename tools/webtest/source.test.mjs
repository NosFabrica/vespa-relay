// The web UI's source files are TEXT — checked here, because for four commits
// one of them was not, and nobody could see it.
//
// searchfield.js carried a literal NUL byte inside a template string (the
// separator in the chip's repaint key, where the character itself is the right
// choice). One byte is enough for git to classify a whole module as binary, and
// every commit touching it then read
//
//     relay/src/main/resources/web/searchfield.js | Bin 23897 -> 23932 bytes
//
// — no diff, in the file or in any review of it. grep answers the same way
// ("binary file matches"), so the module was invisible to search as well.
//
// What makes it worth a test rather than a one-time edit is HOW the file got
// out again: nobody fixed it. git only sniffs the first 8000 bytes, and the
// header comments above the NUL grew until it sat past that line — 7094 bytes
// in, then 7916, then 7950, then 8919, at which point diffs silently came back.
// So the blindness was never repaired, only outrun, and any edit that shortens
// what sits above the byte drags the whole module back over the line. A latent
// property of the file that switches on and off with unrelated content is
// exactly the kind that has to be asserted rather than remembered.
//
// The fix in the module is to write the character as the escape `\u0000`. The
// fix HERE is that the next one cannot be typed in unnoticed. Same rule for
// index.html, which is a source file this suite already reads.

import assert from "assert";
import { readdirSync, readFileSync, statSync } from "node:fs";

const root = new URL("../../web/src/main/resources/", import.meta.url);

/** Every file the page ships, module tree and markup alike. */
function sources(dir, out = []) {
  for (const name of readdirSync(dir)) {
    const at = new URL(name, dir);
    // The trailing slash goes on only once the entry is known to be a
    // directory: statSync on `some-file.html/` is ENOTDIR, not a false.
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

  // What git's own heuristic looks at: a NUL in the first 8000 bytes makes the
  // file binary. Rejected anywhere in it, because the byte is no more readable
  // at offset 9000 and the next edit can move it.
  assert.strictEqual(
    bytes.indexOf(0), -1,
    `${where} contains a NUL byte at offset ${bytes.indexOf(0)} — git will call this file binary and ` +
    `stop diffing it. Write the character as the escape \\u0000 instead.`,
  );

  // The other way a source file stops being one. A broken byte sequence renders
  // as U+FFFD wherever it is read, and a name or a comment quietly loses a
  // character rather than failing.
  assert.doesNotThrow(
    () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
    `${where} is not valid UTF-8`,
  );
}

// The module that paid for this rule, named explicitly: the assertions above
// are a sweep, and a sweep silently covering nothing would still pass.
const field = files.find((f) => f.pathname.endsWith("/web/searchfield.js"));
assert.ok(field, "searchfield.js is not in the swept tree");

console.log(`source: ${files.length} files are text — no NUL bytes, valid UTF-8`);
