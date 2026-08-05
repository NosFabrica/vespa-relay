// Every url on a card comes out of an EVENT — a string a stranger published.
//
// Escaping keeps it inside the attribute; it says nothing about the scheme,
// and `javascript:` in an href runs on click in the page's own origin, which
// is the origin holding this reader's authenticated NIP-42 socket. So the
// rule is checked per FAMILY here rather than trusted per call site: the
// families that render a link are the families that each used to hand-roll
// the same anchor, and that is exactly how five of them ended up with the
// same hole.

import assert from "assert";
globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };

const web = new URL("../../relay/src/main/resources/web/", import.meta.url);
const { card } = await import(new URL("cards.js", web));
const { webUrl, extLink } = await import(new URL("cards/base.js", web));

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";
const now = Math.floor(Date.now() / 1000);
const ev = (kind, tags = [], content = "") => ({ id: "ab".repeat(32), pubkey: pk, kind, created_at: now, tags, content });

// ---- the rule itself -------------------------------------------------------
for (const bad of [
  "javascript:alert(1)",
  "  javascript:alert(1)",
  "JaVaScRiPt:alert(1)",
  "java\nscript:alert(1)",            // the parser strips the newline
  "java\tscript:alert(1)",
  "data:text/html;base64,PHNjcmlwdD4=",
  "vbscript:msgbox(1)",
  "file:///etc/passwd",
  "example.com/no-scheme",            // resolves against our own origin
  "", null, undefined, {},
]) assert.strictEqual(webUrl(bad), null, `webUrl let through: ${String(bad)}`);

for (const good of ["https://example.com/x?a=1&b=2", "http://example.com", "HTTPS://EXAMPLE.COM/x"])
  assert.strictEqual(webUrl(good), good, `webUrl rejected: ${good}`);

// Refused is not dropped: the reader still sees what the event claims.
assert.ok(extLink("javascript:alert(1)").includes("javascript:alert(1)"), "a refused url must still be shown");
assert.ok(!extLink("javascript:alert(1)").includes("<a "), "…but never as a link");
assert.strictEqual(extLink(""), null, "nothing to show is nothing to render");
assert.ok(extLink("https://example.com", "a label").includes(">a label<"), "labels still work");

// ---- every family that renders a url --------------------------------------
// [what it is, the event, where the poison goes]
const POISON = "javascript:alert(document.domain)";
const FAMILIES = [
  ["profile website", ev(0, [], JSON.stringify({ name: "a", website: POISON }))],
  ["app website", ev(31990, [["web", POISON]], JSON.stringify({ name: "app" }))],
  ["repo web", ev(30617, [["name", "r"], ["web", POISON], ["clone", POISON]])],
  ["release artifact", ev(30063, [["title", "v1"], ["url", POISON]])],
  ["live stream", ev(30311, [["title", "t"], ["streaming", POISON], ["recording", POISON]])],
  ["file url", ev(1063, [["url", POISON], ["m", "application/pdf"]])],
  ["video url", ev(21, [["imeta", `url ${POISON}`], ["title", "v"]])],
  ["quote source", ev(9802, [["r", POISON]], "quoted")],
];
for (const [what, event] of FAMILIES) {
  for (const opts of [undefined, { full: true }]) {
    const html = card(event, opts);
    // href only, deliberately. A `javascript:` in an <img>/<video>/<audio>
    // src does not run — the browser simply fails to load it — so holding
    // media to this rule would buy nothing and would cost the data:image
    // embeds that are a legitimate way to carry a thumbnail. The click
    // target is the vector; that is what is checked.
    assert.ok(!/href\s*=\s*["']\s*javascript:/i.test(html), `${what}: rendered a javascript: href`);
    assert.ok(!/href\s*=\s*["']\s*data:/i.test(html), `${what}: rendered a data: href`);
  }
}

// The legitimate case still links, or the check above would pass on a page
// that had simply stopped rendering links at all.
const ok = card(ev(0, [], JSON.stringify({ name: "a", website: "https://example.com/home" })), { full: true });
assert.ok(ok.includes('href="https://example.com/home"'), "a real website must still be a link");
assert.ok(ok.includes('rel="noopener noreferrer"'), "…and must not hand the target our window");

console.log("links: no event can put a scheme of its own in an href");
