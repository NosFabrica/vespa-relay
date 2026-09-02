// THE PAGER, IN A REAL BROWSER — the layer the arithmetic cannot reach.
//
// web/src/test/js/paging.test.mjs holds the rules: where the cut falls, which
// prefix each page asks for, how a widened answer folds into the one on
// screen. Every one of them can be right while the page is wrong, because what
// paging actually is here is WIRING — a preload that must not cancel the
// answer it extends, a url that has to survive Back, a buffer two views used
// to share. That is the shape of both bugs row.probe.mjs was written for, and
// it is why this exists beside it.
//
// Unlike that probe it needs NOTHING but a browser: no Vespa, no corpus, no
// relay. The relay is a fake WebSocket installed before the page's modules
// load, answering any NIP-50 REQ out of a deterministic corpus — `result 0`,
// `result 1`, … — so "page two starts at result 40" is a thing this file can
// assert rather than eyeball. Three query words steer it: `big` is a corpus of
// a thousand (the ask ceiling), `thin` one of seven (no pager at all), and
// `slow` delays every widened ask by 900ms so the reader can be made to outrun
// the preload on purpose.
//
// Playwright is not a dependency of this repo and never should be — the rest
// of the web suite is plain node. Resolved from wherever it is installed,
// global included.
//
//     npm i -g playwright && npx playwright install chromium
//     node web/src/test/browser/pager.probe.mjs
//
// It serves web/src/main/resources itself on :7799, so it runs from a clean
// checkout with nothing else up.
import http from "node:http";
import fs from "node:fs";
import path from "node:path";

const ROOT = "/home/user/vespa-relay/web/src/main/resources";
const TYPES = { ".html": "text/html", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".ico": "image/x-icon" };
const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  let f = url.pathname === "/" ? "/index.html" : url.pathname;
  if (!f.startsWith("/web/") && f !== "/index.html") f = "/index.html";  // the SPA's own routes
  const p = path.join(ROOT, f);
  if (!fs.existsSync(p) || fs.statSync(p).isDirectory()) { res.writeHead(404); res.end(); return; }
  res.writeHead(200, { "content-type": TYPES[path.extname(p)] || "application/octet-stream" });
  res.end(fs.readFileSync(p));
});
await new Promise((r) => server.listen(7799, r));

const { chromium } = await import(`${(await import("node:child_process")).execSync("npm root -g", { encoding: "utf8" }).trim()}/playwright/index.mjs`);
const browser = await chromium.launch();
const page = await browser.newPage();
page.on("console", (m) => { if (m.type() === "error") console.log("  page error:", m.text()); });
page.on("pageerror", (e) => console.log("  PAGE EXCEPTION:", e.message));

const CORPUS = 250;
await page.addInitScript(`
  const CORPUS = ${CORPUS};
  window.__asks = [];
  const hex = (n) => String(n).padStart(64, "0");
  class FakeWS {
    static OPEN = 1;
    constructor(url) {
      this.url = url; this.readyState = 1;
      setTimeout(() => this.onopen && this.onopen(), 0);
    }
    send(raw) {
      const msg = JSON.parse(raw);
      if (msg[0] !== "REQ") return;
      const [, id, ...filters] = msg;
      const f = filters[0] || {};
      const reply = [];
      if (f.search != null) {
        window.__asks.push({ search: f.search, limit: f.limit });
        const size = /big/.test(f.search) ? 1000 : /thin/.test(f.search) ? 7 : CORPUS;
        const n = Math.min(f.limit || 40, size);
        for (let i = 0; i < n; i++) reply.push(["EVENT", id, {
          id: hex(i), pubkey: "b".repeat(64), kind: 1, created_at: 1800000000 - i,
          tags: [], content: "result " + i, sig: "c".repeat(128),
        }]);
      }
      reply.push(["EOSE", id]);
      const slow = /slow/.test(f.search || "") && (f.limit || 0) > 40;
      setTimeout(() => { for (const m of reply) this.onmessage && this.onmessage({ data: JSON.stringify(m) }); }, slow ? 900 : 5);
    }
    close() { this.readyState = 3; }
  }
  window.WebSocket = FakeWS;
`);

const t = async (name, fn) => { try { await fn(); console.log("  ok  " + name); } catch (e) { console.log("  FAIL " + name + ": " + e.message); process.exitCode = 1; } };
// Bounded waits and `.catch(() => {})` wherever a page might NOT render: the
// probe's job is to report which assertion failed, and an unhandled timeout
// takes the whole run down with the state it was about to describe.
const cards = () => page.$$eval(".result", (els) => els.map((e) => e.querySelector(".result-body")?.textContent.trim() || ""));
const pager = () => page.$$eval(".pager .pg", (els) => els.map((e) => `${e.textContent.trim()}${e.disabled ? "(off)" : ""}${e.classList.contains("on") ? "*" : ""}`));
const stats = () => page.$eval(".list-stats", (e) => e.textContent.trim());

await page.goto("http://localhost:7799/?q=test");
await page.waitForSelector(".result");

await t("page one draws one page of cards", async () => {
  const c = await cards();
  if (c.length !== 40 || c[0] !== "result 0" || c[39] !== "result 39") throw new Error(c.length + " " + c[0] + ".." + c[39]);
});
await t("the first ask is one page, not four", async () => {
  const asks = await page.evaluate(() => window.__asks.map((a) => a.limit));
  if (asks[0] !== 40) throw new Error(JSON.stringify(asks));
});
await page.waitForSelector(".pager .pg[data-page='3']", { timeout: 5000 });
await t("the preload asks for three pages more", async () => {
  const asks = await page.evaluate(() => window.__asks.map((a) => a.limit));
  if (!asks.includes(160)) throw new Error(JSON.stringify(asks));
});
await t("…and the pager grows without moving the page on screen", async () => {
  const c = await cards();
  if (c[0] !== "result 0") throw new Error("page one moved: " + c[0]);
  const p = await pager();
  if (JSON.stringify(p) !== JSON.stringify(["‹ Prev(off)", "1*", "2", "3", "4", "…", "Next ›"])) throw new Error(JSON.stringify(p));
});
await t("the head counts the page, not the buffer", async () => {
  const s = await stats();
  if (!/^1–40 · \d+ ms$/.test(s)) throw new Error(s);
});

await page.click(".pager .pg[data-page='1']");
await t("Next draws page two out of the buffer", async () => {
  const c = await cards();
  if (c[0] !== "result 40" || c.length !== 40) throw new Error(c.length + " " + c[0]);
  if (!/^41–80 ·/.test(await stats())) throw new Error(await stats());
});
await t("…and the url is the page", async () => {
  const u = new URL(page.url());
  if (u.searchParams.get("page") !== "2" || u.searchParams.get("q") !== "test") throw new Error(page.url());
});

await page.goBack();
await page.waitForSelector(".result");
await t("Back returns to page one", async () => {
  const c = await cards();
  if (c[0] !== "result 0") throw new Error(c[0]);
  if (new URL(page.url()).searchParams.has("page")) throw new Error(page.url());
});

await page.goto("http://localhost:7799/?q=test&page=4");
await page.waitForSelector(".result");
await t("a deep link opens ON its page", async () => {
  const c = await cards();
  if (c[0] !== "result 120") throw new Error(c[0]);
  const asks = await page.evaluate(() => window.__asks.map((a) => a.limit));
  if (asks[0] !== 160) throw new Error("the first ask must reach the restored page: " + JSON.stringify(asks));
});

// Walk to the end of the corpus.
for (let i = 5; i <= 7; i++) {
  await page.waitForFunction((n) => [...document.querySelectorAll(".pager .pg")].some((e) => e.textContent.trim() === String(n)), i, { timeout: 5000 });
  await page.click(`.pager .pg[data-page='${i - 1}']`);
}
await t("the last page is short and says so", async () => {
  await page.waitForFunction(() => /of 250/.test(document.querySelector(".list-stats")?.textContent || ""), null, { timeout: 5000 });
  const c = await cards();
  if (c.length !== 250 - 240) throw new Error("last page: " + c.length);
  const s = await stats();
  if (!/^241–250 of 250 ·/.test(s)) throw new Error(s);
});
await t("…and Next is spent, with no phantom page past it", async () => {
  const p = await pager();
  if (p[p.length - 1] !== "Next ›(off)") throw new Error(JSON.stringify(p));
  if (p.includes("…")) throw new Error("a page offered past the end of the corpus: " + JSON.stringify(p));
});

// ---- the ceiling ----------------------------------------------------------
await page.goto("http://localhost:7799/?q=big&page=10");
await page.waitForSelector(".result");
await t("the deepest page this view follows a ranking to", async () => {
  const c = await cards();
  if (c[0] !== "result 360") throw new Error(c[0]);
  const asks = await page.evaluate(() => window.__asks.map((a) => a.limit));
  if (Math.max(...asks) > 400) throw new Error("an ask went past the ceiling: " + JSON.stringify(asks));
  const p = await pager();
  if (p[p.length - 1] !== "Next ›(off)" || p.includes("…")) throw new Error(JSON.stringify(p));
  const note = await page.$eval(".pg-note", (e) => e.textContent.trim()).catch(() => "");
  if (!/400 results deep/.test(note)) throw new Error("no note saying WHY it stopped: " + note);
});

// ---- a page that does not exist ------------------------------------------
//
// The regression this file was written too late for: `?page=99` drew the
// skeleton for page 99 and then sat on it forever. settlePage() had corrected
// the state behind it and nothing repainted — caught by pasting exactly this
// url at a real relay, and held here.
await page.goto("http://localhost:7799/?q=test&page=99");
await page.waitForSelector(".result", { timeout: 8000 }).catch(() => {});
await t("a url past the end of the answer lands on the last page there is", async () => {
  const c = await cards();
  if (c[0] !== "result 240" || c.length !== 10) throw new Error(`${c.length} cards from ${c[0]}`);
  if (!/^241–250 of 250 ·/.test(await stats())) throw new Error(await stats());
  if (new URL(page.url()).searchParams.get("page") !== "7") throw new Error("the url still names a page that is not there: " + page.url());
});
await page.goto("http://localhost:7799/?q=thin&page=9");
await page.waitForSelector(".result", { timeout: 8000 }).catch(() => {});
await t("…and one page back is page one, which the url stops naming", async () => {
  if ((await cards()).length !== 7) throw new Error("did not settle onto the only page there is");
  if (new URL(page.url()).searchParams.has("page")) throw new Error(page.url());
});

// ---- an answer that fits on one page has no pager -------------------------
await page.goto("http://localhost:7799/?q=thin");
await page.waitForSelector(".result");
await t("seven results are not a pager", async () => {
  if ((await page.$$(".pager")).length) throw new Error("a pager over a single page");
  if (!/^7 results ·/.test(await stats())) throw new Error(await stats());
});

// ---- the reader outruns the preload ---------------------------------------
await page.goto("http://localhost:7799/?q=slow");
await page.waitForSelector(".result");
await page.click(".pager .pg[data-page='1']");
await t("a page turned before its answer lands draws a skeleton, not 'no results'", async () => {
  if (!(await page.$(".skel-card"))) throw new Error("no skeleton over the page in flight");
  if (await page.$(".empty")) throw new Error("an empty page claimed there were no results");
  if (!/^page 2 ·/.test(await stats())) throw new Error(await stats());
  if (!(await page.$(".pager"))) throw new Error("the way back went missing while waiting");
});
await t("…and fills in when it does", async () => {
  await page.waitForSelector(".result", { timeout: 5000 });
  await page.waitForFunction(() => /^41–80/.test(document.querySelector(".list-stats")?.textContent || ""), null, { timeout: 5000 });
  const c = await cards();
  if (c[0] !== "result 40") throw new Error(c[0]);
});

await t("…and the buffer goes back to being three pages ahead of where they now are", async () => {
  // The turn's own preload() found one already in flight and stood down, so
  // the answer that lands has to ask again or the promise quietly becomes
  // "three pages ahead of where you STARTED".
  await page.waitForFunction(
    () => [...document.querySelectorAll(".pager .pg")].some((e) => e.textContent.trim() === "5"),
    null, { timeout: 6000 },
  );
  const asks = await page.evaluate(() => window.__asks.map((a) => a.limit));
  if (!asks.includes(200)) throw new Error(JSON.stringify(asks));
});

// ---- a page turn names the query the CARDS answer -------------------------
//
// The url is written from `hitsFor`, never from the box, and the difference is
// invisible until somebody starts typing a new search without submitting it —
// at which point a page turn used to write `?q=<what they were typing>&page=2`
// over the results still on screen, and a link to it came back empty.
await page.goto("http://localhost:7799/?q=test");
await page.waitForSelector(".result");
await page.waitForSelector(".pager .pg[data-page='3']", { timeout: 5000 });
await page.click("#q");
await page.keyboard.press("Control+A");
await page.keyboard.type("something else entirely");
await new Promise((r) => setTimeout(r, 400));
await page.click(".pager .pg[data-page='1']");
await t("the url a page turn writes is about the answer, not about the box", async () => {
  const u = new URL(page.url());
  if (u.searchParams.get("q") !== "test") throw new Error(page.url());
  if (u.searchParams.get("page") !== "2") throw new Error(page.url());
  const c = await cards();
  if (c[0] !== "result 40") throw new Error("and the cards are page two of the search that was run: " + c[0]);
});

// ---- a preload that changes nothing but the pager repaints only the pager --
//
// A full re-render destroys and rebuilds every card, which re-arms the lazy
// media observers and drops whatever a browser had started loading in them.
// The marker rides on a card ELEMENT — it cannot survive the list being
// rewritten, which is exactly what makes it the test.
await page.goto("http://localhost:7799/?q=slow");
await page.waitForSelector(".result");
await page.evaluate(() => document.querySelectorAll(".result").forEach((el, i) => (el.dataset.mark = "m" + i)));
await t("the head does not count a page as if it were the whole answer", async () => {
  const s = await stats();
  if (!/^1–40 ·/.test(s)) throw new Error(`"${s}" — a plain count beside a live Next button`);
});
await page.waitForSelector(".pager .pg[data-page='3']", { timeout: 6000 });
await t("…and the cards under the reader are not rebuilt for it", async () => {
  const marks = await page.$$eval(".result[data-mark]", (e) => e.length);
  if (marks !== 40) throw new Error(`${marks} of 40 cards survived the widening ask`);
});

// ---- the type-ahead does not own the results' array -----------------------
await page.goto("http://localhost:7799/?q=test&page=2");
await page.waitForSelector(".result");
await page.click("#q");
await page.keyboard.type("xy");
await page.waitForSelector("#popup.open .popup-item", { timeout: 5000 });
await t("a keystroke over a page of results does not repage them", async () => {
  const c = await cards();
  if (c.length !== 40 || c[0] !== "result 40") throw new Error("the list under the popup moved: " + c.length + " " + c[0]);
});

await browser.close();
server.close();
