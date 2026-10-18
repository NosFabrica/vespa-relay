// Every public page in a real Chromium, under the policy `installPageDefaults` serves: the
// thing PageSecurityTest can only read as text. Serves web/src/main/resources with that
// policy, answers the documents from fixtures and the relay from a fake WebSocket, and fails
// on any policy violation, uncaught error, or a dead picture left on screen.
//
//     node web/src/test/browser/csp.probe.mjs
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const ROOT = new URL("../../main/resources/", import.meta.url).pathname;
const SITE = new URL("../../main/kotlin/com/nosfabrica/vespa/relay/web/StatusSite.kt", import.meta.url).pathname;
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
const PORT = 7803;

/** The policy as the Kotlin constant spells it, so this probe cannot drift from what is served. */
function policy() {
  const src = fs.readFileSync(SITE, "utf8");
  const at = src.indexOf("const val PAGE_CONTENT_SECURITY_POLICY =");
  if (at < 0) throw new Error("StatusSite.kt no longer declares PAGE_CONTENT_SECURITY_POLICY");
  const decl = src.slice(at, src.indexOf("\n\n", at));
  return [...decl.matchAll(/"([^"]*)"/g)].map((m) => m[1]).join("");
}
const CSP = policy();

const now = Math.floor(Date.now() / 1000);
const day = (n) => new Date((now - n * 86400) * 1000).toISOString().slice(0, 10);
const STATS = {
  schema: 2,
  title: "Relay stats",
  scope: "This relay's own store.",
  generatedAt: new Date().toISOString(),
  tiers: { counters: { generatedAt: new Date().toISOString(), everySeconds: 60 } },
  corpus: { status: "ok", data: { events: 123456, newestEvent: now - 60 } },
  authors: { status: "ok", data: { pubkeys: 789 } },
  kinds: { status: "ok", data: { total: 3, all: [{ kind: 1, events: 100000, lastSeen: now - 60 }, { kind: 0, events: 20000, lastSeen: now - 600 }, { kind: 7, events: 3456, lastSeen: now }] } },
  activity: { status: "ok", data: { windowDays: 3, days: [0, 1, 2].map((n) => ({ period: day(n), events: 10 + n, pubkeys: 3 })), hours: [{ hour: 5, events: 4 }] } },
};
const TRUST = { scope: "Ranked search on this relay.", generatedAt: Date.now(), measured: false, steps: [], degradedReads: [] };

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  const headers = { "content-security-policy": CSP, "x-frame-options": "DENY", "x-content-type-options": "nosniff", "referrer-policy": "no-referrer" };
  const json = (doc) => { res.writeHead(200, { ...headers, "content-type": "application/json" }); res.end(JSON.stringify(doc)); };
  if (url.pathname === "/stats.json") return json(STATS);
  if (url.pathname === "/trust.json") return json(TRUST);
  let f = url.pathname === "/" ? "/index.html" : url.pathname;
  if (!f.startsWith("/web/") && !f.endsWith(".html")) f = "/index.html";
  const p = path.join(ROOT, f);
  if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404, headers); res.end(); return; }
  res.writeHead(200, { ...headers, "content-type": TYPES[path.extname(p)] || "application/octet-stream" });
  res.end(fs.readFileSync(p));
});
await new Promise((r) => server.listen(PORT, r));

const { chromium } = await import(`${execSync("npm root -g", { encoding: "utf8" }).trim()}/playwright/index.mjs`);
const browser = await chromium.launch();

// A relay that answers every REQ out of four events: two point at pictures that 404, and one at a live
// picture on another plain-http origin, which an onion or LAN deployment serves its pages beside.
const FAKE_RELAY = `
  const hex = (c) => c.repeat(64);
  const DEAD = location.origin + "/web/nope.png";
  const LIVE_HTTP = "http://127.0.0.1:${PORT}/web/favicon.svg";
  const EVENTS = [
    { id: hex("1"), pubkey: hex("a"), kind: 1, created_at: ${now} - 60, tags: [], content: "csp probe note", sig: hex("c") + hex("c") },
    { id: hex("2"), pubkey: hex("a"), kind: 20, created_at: ${now} - 120, content: "a photo",
      tags: [["title", "dead picture"], ["imeta", "url " + DEAD, "m image/png"]], sig: hex("c") + hex("c") },
    { id: hex("3"), pubkey: hex("a"), kind: 0, created_at: ${now} - 180, tags: [],
      content: JSON.stringify({ name: "probe", picture: DEAD }), sig: hex("c") + hex("c") },
    { id: hex("4"), pubkey: hex("a"), kind: 20, created_at: ${now} - 240, content: "an http photo",
      tags: [["title", "http picture"], ["imeta", "url " + LIVE_HTTP, "m image/svg+xml"]], sig: hex("c") + hex("c") },
  ];
  class FakeWS {
    static OPEN = 1;
    constructor(url) { this.url = url; this.readyState = 1; setTimeout(() => this.onopen && this.onopen(), 0); }
    send(raw) {
      const [verb, id, ...filters] = JSON.parse(raw);
      const out = [];
      if (verb === "COUNT") out.push(["COUNT", id, { count: 0 }]);
      if (verb === "REQ") {
        for (const ev of EVENTS) if (filters.some((f) => !f.kinds || f.kinds.includes(ev.kind))) out.push(["EVENT", id, ev]);
        out.push(["EOSE", id]);
      }
      setTimeout(() => { for (const m of out) this.onmessage && this.onmessage({ data: JSON.stringify(m) }); }, 5);
    }
    close() { this.readyState = 3; }
  }
  window.WebSocket = FakeWS;
`;

let failed = 0;
const ok = (name) => console.log(`  ✓ ${name}`);
const fail = (name, detail) => { failed++; console.error(`  ✗ ${name}${detail ? ` — ${detail}` : ""}`); };

async function visit(file, ready, extra = async () => {}) {
  const page = await browser.newPage();
  const problems = [];
  page.on("pageerror", (e) => problems.push(`uncaught: ${e.message}`));
  page.on("console", (m) => { if (/Content.Security.Policy/i.test(m.text())) problems.push(`console: ${m.text()}`); });
  await page.addInitScript(FAKE_RELAY);
  await page.addInitScript(() => {
    window.__violations = [];
    document.addEventListener("securitypolicyviolation", (e) => window.__violations.push(`${e.violatedDirective} ${e.blockedURI}`));
  });
  const res = await page.goto(`http://localhost:${PORT}/${file}`, { waitUntil: "networkidle" });
  if (!res.headers()["content-security-policy"]) fail(`${file} is served with the policy`);
  await page.waitForSelector(ready, { timeout: 10000 }).catch(() => problems.push(`never drew ${ready}`));
  await extra(page);
  const violations = await page.evaluate(() => window.__violations);
  if (violations.length || problems.length) fail(`${file} runs clean under the policy`, [...violations, ...problems].join("; "));
  else ok(`${file} runs clean under the policy`);

  // The positive control: the policy is live, so an injected handler does not run.
  await page.evaluate(() => document.body.insertAdjacentHTML("beforeend", `<img src="/web/nope.png" onerror="window.__pwned = 1">`));
  await page.waitForTimeout(300);
  const pwned = await page.evaluate(() => window.__pwned);
  const blocked = await page.evaluate(() => window.__violations.some((v) => v.startsWith("script-src")));
  if (pwned || !blocked) fail(`${file} refuses an injected inline handler`, `ran=${!!pwned} reported=${blocked}`);
  else ok(`${file} refuses an injected inline handler`);
  await page.close();
}

await visit("?q=probe", ".result", async (page) => {
  await page.waitForTimeout(800);
  const text = await page.textContent("#results");
  const dead = await page.$$eval("img", (imgs) => imgs.filter((i) => /nope\.png$/.test(i.getAttribute("src") || "")).length);
  if (!/dead picture/.test(text)) fail("the picture's card is drawn", text.slice(0, 200));
  else if (dead) fail("a dead picture is undone by its data-broken mark", `${dead} still on screen`);
  else ok("a dead picture is undone by its data-broken mark");
  const http = await page.$$eval("img", (imgs) => imgs.filter((i) => /^http:\/\/127\.0\.0\.1:/.test(i.getAttribute("src") || "") && i.naturalWidth > 0).length);
  if (!http) fail("a live picture on a plain-http origin is drawn, not refused");
  else ok("a live picture on a plain-http origin is drawn, not refused");
});
await visit("stats.html", ".card");
await visit("trust.html", ".card");
await visit("observer_stats.html", "#foot:not(:empty)");

await browser.close();
server.close();
if (failed) { console.error(`csp: ${failed} failure(s)`); process.exit(1); }
console.log("csp: every public page runs under the policy, and the policy is live");
