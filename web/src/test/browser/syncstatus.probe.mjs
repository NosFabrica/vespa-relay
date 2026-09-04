// The per-relay table in a real Chromium: the wiring sync.test.mjs cannot
// reach (the panel guard in stats.html, the import in cards.js, the member
// names the document and the page must spell the same way). Needs only
// Chromium: it serves web/src/main/resources itself and answers /stats.json
// from the fixture below. SHOT=/tmp/sync.png keeps the picture.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const ROOT = new URL("../../main/resources/", import.meta.url).pathname;
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
const now = Math.floor(Date.now() / 1000);

/**
 * The document as `SyncStatus` publishes it, cut to what this table reads: all
 * four statuses at once. The `statuses` counts do not sum to the rows on
 * purpose; the document cuts `rows` but not the partition the chips read.
 */
const DOC = {
  schema: 1,
  title: "Mirror status",
  generatedAt: new Date().toISOString(),
  tiers: { status: { generatedAt: new Date().toISOString(), sections: ["sync"] } },
  sync: {
    status: "ok",
    generatedAt: new Date().toISOString(),
    data: {
      relays: {
        pairs: 2712,
        statuses: [
          { syncStatus: "refused", pairs: 54 },
          { syncStatus: "notStarted", pairs: 6 },
          { syncStatus: "paging", pairs: 1200 },
          { syncStatus: "complete", pairs: 1452 },
        ],
        freshness: [
          { behind: "current", pairs: 1400 }, { behind: "today", pairs: 900 },
          { behind: "thisWeek", pairs: 300 }, { behind: "older", pairs: 106 }, { behind: "nothing", pairs: 6 },
        ],
        rows: [
          // Complete and nine days cold: a fault on the second axis, sorted first.
          { relay: "wss://cold.example/", stream: "contentViaOutbox", syncStatus: "complete", fault: true,
            behind: "older", behindSec: 9 * 86400, coveredFrom: now - 900 * 86400, coveredTo: now - 9 * 86400,
            verifiedAgoSec: 9 * 86400, negentropy: true },
          { relay: "wss://walled.example/", stream: "contentViaOutbox", syncStatus: "refused", fault: true,
            behind: "nothing",
            refusedFor: "the relay would not accept our NIP-42 identity",
            relaySaid: "auth-required: you are not authorized to perform reqs", refusedAgoSec: 900 },
          { relay: "wss://fresh.example/", stream: "contentViaOutbox", syncStatus: "notStarted", behind: "nothing", fault: true },
          { relay: "wss://deep.example/", stream: "contentViaOutbox", syncStatus: "paging",
            behind: "current", behindSec: 60,
            coveredFrom: now - 200 * 86400, coveredTo: now - 60, asks: 40, bands: 12, settled: 3, visiting: true,
            negentropy: false, kindCap: 8 },
          { relay: "wss://done.example/", stream: "indexers", syncStatus: "complete",
            behind: "current", behindSec: 5,
            coveredFrom: now - 900 * 86400, coveredTo: now - 5, verifiedAgoSec: 41200, tailed: true, negentropy: true },
        ],
        omitted: 1712,
      },
      terms: {
        syncStatus: "Where this router's sync of one (relay, stream) pair stands.",
        coveredFrom: "How far BACK the walk has reached.",
      },
    },
  },
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  if (url.pathname === "/stats.json") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify(DOC));
    return;
  }
  const f = url.pathname === "/" ? "/stats.html" : url.pathname;
  const p = path.join(ROOT, f);
  if (!p.startsWith(ROOT) || !fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
  res.writeHead(200, { "content-type": TYPES[path.extname(p)] || "application/octet-stream" });
  res.end(fs.readFileSync(p));
});
await new Promise((r) => server.listen(7801, r));

// Playwright is not a dependency of this repo; it is resolved from wherever it is installed.
const { chromium } = await import(`${execSync("npm root -g", { encoding: "utf8" }).trim()}/playwright/index.mjs`);
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1500, height: 1400 } });

const problems = [];
// A module that 404s or throws leaves a heading and nothing else, so console errors are failures.
page.on("pageerror", (e) => problems.push(`page error: ${e.message}`));
page.on("console", (m) => { if (m.type() === "error") problems.push(`console: ${m.text()}`); });

await page.goto("http://localhost:7801/", { waitUntil: "networkidle" });
await page.waitForSelector("text=prime relays", { timeout: 10000 });

const ok = (name) => console.log(`  ✓ ${name}`);
const fail = (name, detail) => { console.error(`  ✗ ${name}${detail ? ` — ${detail}` : ""}`); problems.push(name); };
const check = (cond, name, detail) => (cond ? ok(name) : fail(name, detail));

const text = await page.textContent("body");
check(/prime relays/.test(text), "the section is drawn");
check(/1,400 of 2,712 pair\(s\) current/.test(text), "how up to date we are is the headline", text.slice(0, 300));
check(/the past behind it/.test(text), "…and the backfill is the second heading, not the first");

// Five rows are drawn; the chips must say what the router reported, not what the rows add up to.
for (const want of ["54 refused", "6 hasn't started", "1,200 paging", "1,452 complete"]) {
  check(text.includes(want), `the past chip reads "${want}" from the document's own partition`);
}
for (const want of ["1,400 current", "900 today", "300 this week", "106 older", "6 nothing yet"]) {
  check(text.includes(want), `the freshness chip reads "${want}"`);
}

const rows = await page.$$eval("table.sy-legs tr", (trs) =>
  trs.map((tr) => ({ hot: tr.classList.contains("hot"), cells: [...tr.querySelectorAll("td")].map((td) => td.textContent.trim()) }))
     .filter((r) => r.cells.length));
const byRelay = Object.fromEntries(rows.filter((r) => r.cells[0]).map((r) => [r.cells[0], r]));
check(!!byRelay["cold.example/"], "the stale-but-complete relay has a row", Object.keys(byRelay).join(" | "));
check(!!byRelay["walled.example/"], "the refused relay has a row", Object.keys(byRelay).join(" | "));
check(!!byRelay["fresh.example/"], "…and so does the one never reached");
check(!!byRelay["deep.example/"], "…and the one still paging");
check(!!byRelay["done.example/"], "…and the one that is finished");

check(/auth-required: you are not authorized/.test(text), "the relay's own words reach the page");
check(/would not accept our NIP-42 identity/.test(text), "…beside the router's reading of which wall it is");

check(byRelay["cold.example/"]?.hot === true, "a complete pair nine days cold is coloured");
check(byRelay["walled.example/"]?.hot === true, "the refused row is coloured");
check(byRelay["fresh.example/"]?.hot === true, "the never-started row is coloured");
check(byRelay["deep.example/"]?.hot === false, "the paging row is NOT coloured — a mirror working is not a fault");
check(byRelay["done.example/"]?.hot === false, "and neither is a current, finished one");

const first = rows.find((r) => r.cells[0])?.cells[0];
check(first === "cold.example/", "the faults are first, across both axes", `first row: ${first}`);

check(/no neg/.test(text), "a relay that cannot reconcile says so");
check(/≤8 kinds/.test(text), "…and one whose filter width we learned says that");
check(/9d old/.test(text), "the newest event's age reads in days, not 216 hours");
// The relay's sentence is the one cell that wraps, and the one an operator needs whole.
const overflow = await page.$$eval("table.sy-legs", (ts) => ts.map((t) => t.scrollWidth - t.clientWidth));
check(overflow.every((o) => o <= 1), "the table fits its card", `overflow: ${overflow.join(",")}`);

check(/3\/40/.test(text), "a paging row shows the settled fraction");

check(!/1970/.test(text), "no epoch dates anywhere on the page");
check((byRelay["fresh.example/"]?.cells || []).includes("—"), "a pair with no coverage draws dashes");

check(/1,712 more pair\(s\) not listed/.test(text), "the truncation is disclosed");

if (process.env.SHOT) {
  await page.screenshot({ path: process.env.SHOT, fullPage: true });
  console.log(`  → ${process.env.SHOT}`);
}

await browser.close();
server.close();
if (problems.length) { console.error(`\n${problems.length} problem(s):\n  ${problems.join("\n  ")}`); process.exit(1); }
console.log("\nsyncstatus.probe: the per-relay table renders, and reads what the document says.");
