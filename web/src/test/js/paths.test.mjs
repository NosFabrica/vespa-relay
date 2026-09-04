// Every asset reference a page makes is document-relative and resolves. The
// status pages are mounted under path prefixes (`/sync/`, `/monitor/`) with a
// strip rewrite; an absolute `/web/…` reference is answered by the relay at
// the host root with the wrong service's module, and nothing 404s. A bare
// `web/shared/page.js` specifier is an import map key, not a relative url,
// and fails the whole module graph. Anchors are navigation, not assets, and
// are not covered.
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

const pages = readdirSync(RES).filter((f) => f.endsWith(".html"));
if (!pages.length) throw new Error(`no pages found under ${RES} — this suite would pass by finding nothing`);

let checked = 0;
for (const page of pages) {
  const html = readFileSync(path.join(RES, page), "utf8");

  for (const m of html.matchAll(/<(link|script)\b[^>]*\b(?:href|src)="([^"]+)"/g)) {
    const url = m[2];
    if (/^[a-z][a-z0-9+.-]*:|^\/\//i.test(url)) continue;
    checked++;
    if (url.startsWith("/")) fail(`${page}: <${m[1]}> points at the host root — ${url}`);
    else if (!existsSync(path.join(RES, path.posix.normalize(url)))) fail(`${page}: <${m[1]}> ${url} does not resolve`);
  }

  // The inline module's specifiers resolve against the document base url.
  for (const m of html.matchAll(/(?:^|[\s;{(])(?:import|export)[^;]*?\bfrom\s+["']([^"']+)["']/g)) {
    const spec = m[1];
    checked++;
    if (spec.startsWith("/")) fail(`${page}: imports through an absolute specifier — ${spec}`);
    else if (!/^\.{1,2}\//.test(spec)) fail(`${page}: "${spec}" is a bare specifier, not a relative url — it needs a leading ./`);
    else if (!existsSync(path.join(RES, path.posix.normalize(spec)))) fail(`${page}: imports ${spec}, which does not resolve`);
  }
}

// A module's specifiers resolve against its own url, so only an absolute one can break.
for (const file of walk(path.join(RES, "web")).filter((f) => f.endsWith(".js"))) {
  const rel = path.relative(RES, file);
  const src = readFileSync(file, "utf8");
  for (const m of src.matchAll(/(?:^|[\s;{(])(?:import|export)[^;]*?\bfrom\s+["']([^"']+)["']/g)) {
    checked++;
    if (m[1].startsWith("/")) fail(`${rel}: imports through an absolute specifier — ${m[1]}`);
  }
  // Quoted, so prose naming the endpoint in backticks does not match.
  for (const m of src.matchAll(/["']\/stats\.json["']/g)) {
    fail(`${rel}: fetches ${m[0]} from the host root, which is another service's document behind a prefix`);
  }
}

if (failures.length) {
  console.error(failures.map((f) => `  ${f}`).join("\n"));
  throw new Error(`${failures.length} reference(s) that would break a path-prefix mount`);
}
console.log(`paths: ${checked} references across ${pages.length} pages and their modules are document-relative and resolve`);
