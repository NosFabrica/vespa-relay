// Every asset reference a page makes is DOCUMENT-RELATIVE, and resolves.
//
// The three status pages are one file served by the relay, the mirror and the
// monitor. An operator putting all three behind a single hostname mounts two of
// them under a path prefix — `/sync/`, `/monitor/` — with a plain strip
// rewrite, and the whole question of whether that works is what the markup ASKS
// FOR. The routes are unchanged and still absolute: `GET /web/{path...}` and
// `GET /stats.json` are where the server answers.
//
// An absolute `/web/shared/page.js` is asked of the host ROOT wherever the page
// itself was mounted. That is not a 404 on the deployment that matters: the
// relay sits at that root and serves its own copy of every file name under
// `/web/`, so the reader gets 200s, a page that renders its chrome, and the
// wrong service's modules drawn silently. This suite is what keeps a reference
// from drifting back — it is exactly the failure that shows up nowhere else.
//
// Three things are checked, and the third is the one nobody would guess:
//
//   1. no `<link>`/`<script>` in the markup points at the host root;
//   2. no module in `web/` imports through an absolute specifier;
//   3. every module specifier in the markup begins with `./` or `../`. A bare
//      `web/shared/page.js` is NOT a relative url — it is an import map key,
//      and a browser with no import map fails the entire module graph on it.
//      It looks right in a diff and 404s nothing; it simply never loads.
//
// Anchors are deliberately not covered. `<a href="/">` is navigation inside a
// root-anchored SPA, not an asset the page needs to load.
import { readFileSync, existsSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const RES = fileURLToPath(new URL("../../main/resources/", import.meta.url));

function walk(dir) {
  const out = [];
  for (const e of readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) out.push(...walk(p));
    else out.push(p);
  }
  return out;
}

const failures = [];
const fail = (m) => failures.push(m);

// ── 1 & 3: the markup ────────────────────────────────────────────────────────
const pages = readdirSync(RES).filter((f) => f.endsWith(".html"));
if (!pages.length) throw new Error(`no pages found under ${RES} — this suite would pass by finding nothing`);

let checked = 0;
for (const page of pages) {
  const html = readFileSync(path.join(RES, page), "utf8");

  // Everything the BROWSER is told to fetch to draw the page: stylesheets,
  // icons, preload hints, script sources.
  for (const m of html.matchAll(/<(link|script)\b[^>]*\b(?:href|src)="([^"]+)"/g)) {
    const url = m[2];
    if (/^[a-z][a-z0-9+.-]*:|^\/\//i.test(url)) continue; // an off-site url is not ours to resolve
    checked++;
    if (url.startsWith("/")) fail(`${page}: <${m[1]}> points at the host root — ${url}`);
    else if (!existsSync(path.join(RES, path.posix.normalize(url)))) fail(`${page}: <${m[1]}> ${url} does not resolve`);
  }

  // The inline module's own specifiers. These resolve against the DOCUMENT base
  // url, which is what makes the whole arrangement work — and what makes the
  // `./` mandatory.
  for (const m of html.matchAll(/(?:^|[\s;{(])(?:import|export)[^;]*?\bfrom\s+["']([^"']+)["']/g)) {
    const spec = m[1];
    checked++;
    if (spec.startsWith("/")) fail(`${page}: imports through an absolute specifier — ${spec}`);
    else if (!/^\.{1,2}\//.test(spec)) fail(`${page}: "${spec}" is a bare specifier, not a relative url — it needs a leading ./`);
    else if (!existsSync(path.join(RES, path.posix.normalize(spec)))) fail(`${page}: imports ${spec}, which does not resolve`);
  }
}

// ── 2: the modules ───────────────────────────────────────────────────────────
// Their own specifiers resolve against the importing module's url, so relative
// is prefix-safe here for free — the only thing that can break it is an
// absolute one.
for (const file of walk(path.join(RES, "web")).filter((f) => f.endsWith(".js"))) {
  const rel = path.relative(RES, file);
  const src = readFileSync(file, "utf8");
  for (const m of src.matchAll(/(?:^|[\s;{(])(?:import|export)[^;]*?\bfrom\s+["']([^"']+)["']/g)) {
    checked++;
    if (m[1].startsWith("/")) fail(`${rel}: imports through an absolute specifier — ${m[1]}`);
  }
  // The document a status page charts, asked for the same way and for the same
  // reason. Quoted, so the prose that names the endpoint in backticks is not
  // mistaken for a reference to it.
  for (const m of src.matchAll(/["']\/stats\.json["']/g)) {
    fail(`${rel}: fetches ${m[0]} from the host root, which is another service's document behind a prefix`);
  }
}

if (failures.length) {
  console.error(failures.map((f) => `  ${f}`).join("\n"));
  throw new Error(`${failures.length} reference(s) that would break a path-prefix mount`);
}
console.log(`paths: ${checked} references across ${pages.length} pages and their modules are document-relative and resolve`);
