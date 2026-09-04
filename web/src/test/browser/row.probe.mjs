// The provenance row in a real Chromium against a real relay and an observer
// corpus: the wiring the module tests cannot reach. Run by hand, never by
// `./gradlew build`. The observer is passed as `as=` rather than signed in, so
// a green run says nothing about the reader's own NIP-51 lists (see
// docs/decisions/tests-web-b.md).
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
// Playwright is not a dependency of this repo; it is resolved from wherever it is installed.
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
// The row lands on a late repaint, so every wait is for the pills, never a fixed sleep.
const waitForPills = async (ms = 30000) => {
  try { await page.waitForFunction(() => document.querySelectorAll(".prov-pill").length > 0, null, { timeout: ms }); }
  catch { /* the assertion below says it */ }
};

console.log(`\nrow.probe — ${BASE}, ranking as ${AS.slice(0, 12)}…, query ${JSON.stringify(QUERY)}`);

console.log("\nthe results list");
await page.goto(`${BASE}/?q=${encodeURIComponent(QUERY)}&tab=people&as=${AS}`, { waitUntil: "domcontentloaded" });
await waitForPills();
const onResults = await pills();
ok(onResults > 0, `a People search draws the row it no longer gets for free (${onResults} pills)`);
const firstText = onResults ? await page.locator(".prov-pill").first().innerText() : "";
ok(!/^#[a-z]$/.test(firstText.trim()), `a pill says something a reader can read (first: ${JSON.stringify(firstText.trim())})`);

// The popup's hydrate shares the provenance map with the results view.
console.log("\nthe type-ahead popup, over the same results");
await page.click("#q").catch(() => {});
await page.keyboard.type(" bit", { delay: 60 });   // a debounced popup search, mid-page
await page.waitForTimeout(4000);
const afterTyping = await pills();
ok(afterTyping > 0, `typing into the box leaves the row standing (${afterTyping} pills)`);

// The entity view clears the row on the way in and must ask again for its
// own subject; `related` is appended after the card, so the answer lands in the card.
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

// The lens is part of the URL; a pasted permalink must read it before ranking.
console.log("\nthe same permalink, pasted cold");
if (href) {
  await page.goto(`${BASE}${href}?as=${AS}`, { waitUntil: "domcontentloaded" });
  await waitForPills();
  ok(await pills() > 0, `a permalink carrying \`as=\` draws the row (${await pills()} pills)`);
}

console.log("\nthe console");
ok(errors.length === 0, `no page errors (${errors.length}${errors.length ? ": " + errors[0].slice(0, 120) : ""})`);

await browser.close();
console.log(failed ? `\n${failed} FAILED\n` : "\nall assertions passed\n");
process.exit(failed ? 1 : 0);
