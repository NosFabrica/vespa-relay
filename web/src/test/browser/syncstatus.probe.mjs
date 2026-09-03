// THE PER-RELAY TABLE, IN A REAL BROWSER — the layer the rules cannot reach.
//
// web/src/test/js/sync.test.mjs holds every decision this table makes: which
// denominator the chips are read off, which rows are coloured, what an absent
// edge renders as. All of them can be right while the page draws nothing, and
// in this repository they have been — twice, per AGENTS.md, and neither bug was
// in a rule. What a new section actually IS is wiring: a panel guard in
// stats.html, an import in cards.js, a member name the document and the page
// have to spell the same way. Only a browser resolves that.
//
// It caught one on the way in. The sync panel was guarded on
// `progress || streams`, so a mirror whose whole roster is refused — no bands,
// no walked streams, which is precisely the deployment this table exists for —
// drew no card at all.
//
// Needs NOTHING but Chromium: it serves web/src/main/resources itself and
// answers /stats.json out of the fixture below, so it runs from a clean
// checkout with no Vespa, no router and no corpus. Asserts, and exits non-zero
// on the first failure.
//
//     node web/src/test/browser/syncstatus.probe.mjs
//     …and to keep the picture:  SHOT=/tmp/sync.png node …
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { execSync } from "node:child_process";

const ROOT = new URL("../../main/resources/", import.meta.url).pathname;
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
const now = Math.floor(Date.now() / 1000);

/**
 * The document as `SyncStatus` publishes it, cut to what this table reads.
 *
 * Deliberately a mirror in trouble: a refused pair, one never reached, one
 * paging and one complete — the four statuses at once, which is the only
 * fixture that can show the table telling them apart. The `statuses` counts do
 * NOT sum to the four rows, on purpose: the document cuts `rows` and does not
 * cut the partition, and the chips must come off the partition.
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
        rows: [
          { relay: "wss://walled.example/", stream: "contentViaOutbox", syncStatus: "refused",
            refusedFor: "the relay would not accept our NIP-42 identity",
            relaySaid: "auth-required: you are not authorized to perform reqs", refusedAgoSec: 900 },
          { relay: "wss://fresh.example/", stream: "contentViaOutbox", syncStatus: "notStarted" },
          { relay: "wss://deep.example/", stream: "contentViaOutbox", syncStatus: "paging",
            coveredFrom: now - 200 * 86400, coveredTo: now - 60, asks: 40, bands: 3, visiting: true },
          { relay: "wss://done.example/", stream: "indexers", syncStatus: "complete",
            coveredFrom: now - 900 * 86400, coveredTo: now - 5, verifiedAgoSec: 41200, tailed: true },
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

// Playwright is not a dependency of this repo and never should be — the rest
// of the web suite is plain node. Resolved from wherever it is installed.
const { chromium } = await import(`${execSync("npm root -g", { encoding: "utf8" }).trim()}/playwright/index.mjs`);
const browser = await chromium.launch();
const page = await browser.newPage({ viewport: { width: 1500, height: 1400 } });

const problems = [];
// A module that 404s or throws leaves a heading and nothing else, which is
// exactly what this probe is for — so console errors are failures, not noise.
page.on("pageerror", (e) => problems.push(`page error: ${e.message}`));
page.on("console", (m) => { if (m.type() === "error") problems.push(`console: ${m.text()}`); });

await page.goto("http://localhost:7801/", { waitUntil: "networkidle" });
await page.waitForSelector("text=prime relays", { timeout: 10000 });

const ok = (name) => console.log(`  ✓ ${name}`);
const fail = (name, detail) => { console.error(`  ✗ ${name}${detail ? ` — ${detail}` : ""}`); problems.push(name); };
const check = (cond, name, detail) => (cond ? ok(name) : fail(name, detail));

// THE HEADING AND THE TABLE UNDER IT. The card is guarded on the section, so
// this failing means the panel guard, the import or the member name is wrong —
// the three things no unit test in this repository can see.
const text = await page.textContent("body");
check(/prime relays/.test(text), "the section is drawn");
check(/2,712 prime relay\/stream pair\(s\)/.test(text), "the roster's size is the heading", text.slice(0, 200));

// THE CHIPS COME OFF THE PARTITION. Four rows are drawn; the chips must say
// 54 / 6 / 1,200 / 1,452 — read off `statuses`, not counted off the rows.
for (const want of ["54 refused", "6 hasn't started", "1,200 paging", "1,452 complete"]) {
  check(text.includes(want), `the chip reads "${want}" from the document's own partition`);
}

// EVERY ROW, AND THE FOUR STATUSES TOLD APART.
const rows = await page.$$eval("table.sy-legs tr", (trs) =>
  trs.map((tr) => ({ hot: tr.classList.contains("hot"), cells: [...tr.querySelectorAll("td")].map((td) => td.textContent.trim()) }))
     .filter((r) => r.cells.length));
const byRelay = Object.fromEntries(rows.filter((r) => r.cells[0]).map((r) => [r.cells[0], r]));
check(!!byRelay["walled.example/"], "the refused relay has a row", Object.keys(byRelay).join(" | "));
check(!!byRelay["fresh.example/"], "…and so does the one never reached");
check(!!byRelay["deep.example/"], "…and the one still paging");
check(!!byRelay["done.example/"], "…and the one that is finished");

// THE RELAY'S OWN SENTENCE, which is the whole reason the refusal cell is a
// cell and not a chip: it is what says what to do about the wall.
check(/auth-required: you are not authorized/.test(text), "the relay's own words reach the page");
check(/would not accept our NIP-42 identity/.test(text), "…beside the router's reading of which wall it is");

// ONLY THE TWO THAT NAME A FAULT ARE COLOURED. `paging` is a mirror working.
check(byRelay["walled.example/"]?.hot === true, "the refused row is coloured");
check(byRelay["fresh.example/"]?.hot === true, "the never-started row is coloured");
check(byRelay["deep.example/"]?.hot === false, "the paging row is NOT coloured — a mirror working is not a fault");
check(byRelay["done.example/"]?.hot === false, "and neither is a finished one");

// AN ABSENT EDGE RENDERS AS A DASH, never as 1970 — the one failure a reader
// would act on and be wrong about.
check(!/1970/.test(text), "no epoch dates anywhere on the page");
check((byRelay["fresh.example/"]?.cells || []).includes("—"), "a pair with no coverage draws dashes");

// AND THE CUT DISCLOSES ITSELF.
check(/1,712 more pair\(s\) not listed/.test(text), "the truncation is disclosed");

if (process.env.SHOT) {
  await page.screenshot({ path: process.env.SHOT, fullPage: true });
  console.log(`  → ${process.env.SHOT}`);
}

await browser.close();
server.close();
if (problems.length) { console.error(`\n${problems.length} problem(s):\n  ${problems.join("\n  ")}`); process.exit(1); }
console.log("\nsyncstatus.probe: the per-relay table renders, and reads what the document says.");
