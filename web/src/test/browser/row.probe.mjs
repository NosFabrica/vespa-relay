// THE PROVENANCE ROW, IN A REAL BROWSER — the layer every other test on this
// branch skips, and the one both of its bugs lived in.
//
// web/src/test/js covers the RULES (provenance.js, pointers.js, providers.js)
// and it covered them while the page drew nothing at all, twice. Neither bug
// was in a rule:
//
//   - hydrate() is shared by three views and seeds one global map, so the
//     type-ahead popup — which cannot draw a pill — replaced the results
//     view's row on every keystroke and the late fetch came back to a guard
//     that said "stale";
//   - the entity page cleared the row on the way in and never asked again, so
//     a permalink drew none.
//
// Both are wiring, and wiring needs the wiring. So: a real Chromium, a real
// relay, a real corpus, and the four assertions the modules cannot make.
//
// It is a PROBE, not part of `./gradlew build`: it needs a Vespa, a corpus
// that is not in the repo, and a browser. Same bargain as ObserverTrustListIT.
//
//   docker run -d --name vespa -m 9g -p 127.0.0.1:8080:8080 \
//       -p 127.0.0.1:19071:19071 vespaengine/vespa
//   node relay/tools/fetch-observer-corpus.mjs /tmp/obs <observer-hex>
//   ./gradlew :relay:test --tests '*ObserverTrustListIT*' \
//       -DitVespa=http://localhost:8080 -DitCorpus=/tmp/obs      # loads it
//   ./gradlew :relay:installDist && VESPA_URL=http://localhost:8080 \
//       RELAY_URL=ws://localhost:7777 AUTO_DEPLOY=false \
//       relay/build/install/vespa-relay/bin/vespa-relay &
//   node web/src/test/browser/row.probe.mjs http://localhost:7777 <observer-npub> "<a list title>"
//
// The observer is passed as `as=` rather than signed in: the lens is public,
// so `viewingAs` reaches the same code without a NIP-07 extension.
// Playwright is not a dependency of this repo and never should be — the rest
// of the web suite is plain node on purpose. Resolved from wherever it happens
// to be installed, global included, rather than pinned to one machine's path.
const { chromium } = await import("playwright").catch(async () => {
  const { execSync } = await import("node:child_process");
  const root = execSync("npm root -g", { encoding: "utf8" }).trim();
  return import(`${root}/playwright/index.mjs`);
});

const BASE = process.argv[2] || "http://localhost:7777";
const AS = process.argv[3];
const QUERY = process.argv[4] || "AOS 2026 Participant";
if (!AS || !/^npub1[0-9a-z]+$/.test(AS)) {
  console.error("usage: row.probe.mjs <relay-http-url> <observer-npub> [query]");
  process.exit(2);
}

let failed = 0;
const ok = (cond, msg) => { console.log(`  ${cond ? "PASS" : "**FAIL**"}  ${msg}`); if (!cond) failed++; };

const browser = await chromium.launch();
const page = await browser.newPage();
const errors = [];
page.on("pageerror", (e) => errors.push(e.message));
page.on("console", (m) => {
  if (m.type() !== "error") return;
  const t = m.text();
  if (/Failed to load resource|net::ERR_/.test(t)) return;   // an avatar's host, not this page
  errors.push(t);
});

const pills = () => page.locator(".prov-pill").count();
// The row lands on a late repaint by design, so every wait here is for the
// PILLS, never a fixed sleep — a sleep is how a slow relay turns this green.
const waitForPills = async (ms = 30000) => {
  try { await page.waitForFunction(() => document.querySelectorAll(".prov-pill").length > 0, null, { timeout: ms }); }
  catch { /* the assertion below says it */ }
};

console.log(`\nrow.probe — ${BASE}, ranking as ${AS.slice(0, 12)}…, query ${JSON.stringify(QUERY)}`);

// ---- 1. the results list draws a row at all -------------------------------
console.log("\nthe results list");
await page.goto(`${BASE}/?q=${encodeURIComponent(QUERY)}&tab=people&as=${AS}`, { waitUntil: "domcontentloaded" });
await waitForPills();
const onResults = await pills();
ok(onResults > 0, `a People search draws the row it no longer gets for free (${onResults} pills)`);
const firstText = onResults ? await page.locator(".prov-pill").first().innerText() : "";
ok(!/^#[a-z]$/.test(firstText.trim()), `a pill says something a reader can read (first: ${JSON.stringify(firstText.trim())})`);

// ---- 2. …and typing does not take it away ---------------------------------
//
// The bug: the popup's hydrate seeded the one global provenance map with its
// own eight rows and moved the epoch, so the search's late fetch was dropped.
// It fires on every keystroke, so this was every search.
console.log("\nthe type-ahead popup, over the same results");
await page.click("#q").catch(() => {});
await page.keyboard.type(" bit", { delay: 60 });   // a debounced popup search, mid-page
await page.waitForTimeout(4000);
const afterTyping = await pills();
ok(afterTyping > 0, `typing into the box leaves the row standing (${afterTyping} pills)`);

// ---- 3. clicking through to a profile keeps the row -----------------------
//
// The journey the reader actually takes, and the one that was reported empty.
// The entity view clears on the way in — a row inherited from the last search
// would mean "how you got here" — so it has to ask again for the entity
// itself, and `related` is appended after the card, so the answer has to land
// in the card without taking that with it.
console.log("\nclicking through to a profile");
await page.goto(`${BASE}/?q=${encodeURIComponent(QUERY)}&tab=people&as=${AS}`, { waitUntil: "domcontentloaded" });
await waitForPills();
const link = page.locator("a[href^='/npub1']").first();
const href = await link.getAttribute("href").catch(() => null);
if (!href) { ok(false, "found a profile card to click through to"); }
else {
  await link.click();
  await page.waitForFunction(() => !!document.getElementById("entity-card"), null, { timeout: 20000 }).catch(() => {});
  await waitForPills();
  ok(await pills() > 0, `the profile it opened draws the row for its own subject (${await pills()} pills)`);
  ok(await page.evaluate(() => !!document.getElementById("entity-card")),
    "and it lands in the card's slot, so `related` beneath it survives");
}

// ---- 4. …and so does the same permalink pasted cold ------------------------
//
// The lens is part of the URL, and this branch used to return before reading
// it — so a pasted `/npub1…?as=…` ranked as nobody, which is no delegations,
// which is no row. It cost the score chips their numbers here too, long before
// the row existed.
console.log("\nthe same permalink, pasted cold");
if (href) {
  await page.goto(`${BASE}${href}?as=${AS}`, { waitUntil: "domcontentloaded" });
  await waitForPills();
  ok(await pills() > 0, `a permalink carrying \`as=\` draws the row (${await pills()} pills)`);
}

// ---- 5. nothing threw on the way ------------------------------------------
//
// PAGE errors only. Avatars are fetched from whatever host a profile names,
// and a sandbox that cannot reach them is not this page misbehaving.
console.log("\nthe console");
ok(errors.length === 0, `no page errors (${errors.length}${errors.length ? ": " + errors[0].slice(0, 120) : ""})`);

await browser.close();
console.log(failed ? `\n${failed} FAILED\n` : "\nall assertions passed\n");
process.exit(failed ? 1 : 0);
