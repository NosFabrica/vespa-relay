// The modulepreload hints in index.html must match what app.js actually reaches.
//
// They exist to collapse a three-wave module waterfall into one round trip. A
// hint for a module that no longer exists is a wasted 404; a module with no
// hint quietly returns to the slow path. Neither shows up as a failure anywhere
// else, so it is asserted here.
import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

// Resolved from THIS FILE, not from the working directory. Gradle runs the
// suite from the module root and a human runs it from wherever they are; a
// crawler that silently finds no files reports a graph with nothing in it and
// calls the hints stale.
const RES = fileURLToPath(new URL("../../main/resources/", import.meta.url));
// Resource-root-relative, with no leading slash, because that is what both
// sides of this comparison normalise to: the hints in the markup are written
// document-relative (`./web/…`, so the page survives a path-prefix mount — see
// stats.html's head), and the crawler walks the file tree.
const entry = "web/app.js";

const reached = new Set();
const queue = [entry];
while (queue.length) {
  const u = queue.shift();
  if (reached.has(u)) continue;
  reached.add(u);
  const f = path.join(RES, u);
  if (!existsSync(f)) continue;
  // Both import forms. Matching only `from "…"` missed every side-effect
  // import — which is precisely how cards.js pulls in the kind registry, so
  // the family modules were invisible to this crawler and unhinted in
  // index.html while it reported the graph as matching "exactly". The modules
  // that must load before a single result can render were the ones left on
  // the slow path.
  for (const m of readFileSync(f, "utf8").matchAll(/(?:from|import)\s+["']([^"']+)["']/g)) {
    if (!m[1].startsWith(".")) continue;
    const abs = path.posix.normalize(path.posix.join(path.posix.dirname(u), m[1]));
    if (abs.endsWith(".js")) queue.push(abs);
  }
}
reached.delete(entry);

const html = readFileSync(path.join(RES, "index.html"), "utf8");
const hinted = new Set(
  [...html.matchAll(/<link rel="modulepreload" href="([^"]+)"/g)].map((m) => path.posix.normalize(m[1])),
);

const missing = [...reached].filter((m) => !hinted.has(m)).sort();
const stale = [...hinted].filter((m) => !reached.has(m)).sort();

if (missing.length) throw new Error(`modules reached but NOT preloaded (slow path): ${missing.join(", ")}`);
if (stale.length) throw new Error(`preloaded but unreachable (wasted request): ${stale.join(", ")}`);
console.log(`modulepreload: ${hinted.size} hints match the import graph exactly`);
