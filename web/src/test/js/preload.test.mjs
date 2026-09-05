// The modulepreload hints in index.html must match what app.js reaches: a stale hint is a
// wasted 404, a missing one puts the module back on the slow path.
import { readFileSync, existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

// Resolved from this file, not the working directory, or an empty graph calls the hints stale.
const RES = fileURLToPath(new URL("../../main/resources/", import.meta.url));
// Resource-root-relative with no leading slash, which both the hints and the file walk normalise to.
const entry = "web/app.js";

const reached = new Set();
const queue = [entry];
while (queue.length) {
  const u = queue.shift();
  if (reached.has(u)) continue;
  reached.add(u);
  const f = path.join(RES, u);
  if (!existsSync(f)) continue;
  // Both import forms: side-effect imports are how cards.js pulls in the kind registry.
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
