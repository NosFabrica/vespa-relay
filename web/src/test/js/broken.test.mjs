// A failed load is undone by one delegated listener, not by inline `onerror`: the pages are
// served under `script-src 'self'`, which refuses to run an inline handler, so one in a
// renderer's markup would leave every dead picture as a broken box, in a browser only.
import assert from "node:assert/strict";
import { readFileSync, readdirSync } from "node:fs";
import { fileURLToPath } from "node:url";
import path from "node:path";

const { BROKEN, onBroken, watchBrokenMedia } = await import("../../main/resources/web/shared/broken.js");
const { BLANK, avatarHtml } = await import("../../main/resources/web/shared/avatar.js");

const ok = (name) => console.log(`  ✓ ${name}`);

/** Just enough of an element: a dataset, attributes, a class list, a parent that can leave. */
function fakeEl(broken, attrs = {}) {
  const parent = { removed: false, remove() { this.removed = true; } };
  const classes = new Set();
  const el = {
    dataset: broken ? { broken } : {},
    attrs: { ...attrs },
    parentElement: parent,
    removed: false,
    classList: { add: (c) => classes.add(c), contains: (c) => classes.has(c) },
    getAttribute(n) { return this.attrs[n] ?? null; },
    setAttribute(n, v) { this.attrs[n] = v; },
    remove() { this.removed = true; },
  };
  return { el, parent };
}

{
  const { el, parent } = fakeEl(BROKEN.self);
  assert.equal(onBroken(el), true);
  assert(el.removed && !parent.removed, "a grid cell goes alone");

  const framed = fakeEl(BROKEN.frame);
  onBroken(framed.el);
  assert(framed.parent.removed, "a lone picture takes its frame with it");

  const face = fakeEl(BROKEN.face, { src: "https://x/p.png" });
  onBroken(face.el);
  assert.equal(face.el.getAttribute("src"), BLANK, "a face falls back in place");
  assert(face.el.classList.contains("gen") && !face.el.removed, "to the generated face, not to nothing");
  // A second error on the blank itself must not set it again, or a failing blank would loop.
  face.el.setAttribute = () => assert.fail("re-set the blank");
  onBroken(face.el);

  const plain = fakeEl(null);
  assert.equal(onBroken(plain.el), false, "an unmarked element is left alone");
  assert.equal(onBroken(null), false);
  ok("each mark undoes its own failed load, and nothing else is touched");
}

{
  let seen = null;
  const doc = { addEventListener: (type, fn, capture) => (seen = { type, fn, capture }) };
  watchBrokenMedia(doc);
  assert.equal(seen.type, "error");
  assert.equal(seen.capture, true, "`error` does not bubble: only a capture listener on the document hears it");
  const { el } = fakeEl(BROKEN.self);
  seen.fn({ target: el });
  assert(el.removed);
  ok("one capture-phase listener on the document hears every failed load");
}

{
  // Every renderer's markup, read as source: a handler attribute inside a string or template.
  const WEB = fileURLToPath(new URL("../../main/resources/web/", import.meta.url));
  const walk = (dir) => readdirSync(dir, { withFileTypes: true }).flatMap((e) =>
    e.isDirectory() ? walk(path.join(dir, e.name)) : e.name.endsWith(".js") ? [path.join(dir, e.name)] : []);
  const files = walk(WEB);
  assert(files.length > 50, `found ${files.length} modules — this check would pass by finding nothing`);
  const offenders = [];
  for (const f of files) {
    const src = readFileSync(f, "utf8");
    for (const m of src.matchAll(/<[a-z][^<>]*\son[a-z]+\s*=/gi)) offenders.push(`${path.relative(WEB, f)}: ${m[0]}`);
    if (/href=["']?javascript:/i.test(src)) offenders.push(`${path.relative(WEB, f)}: a javascript: url`);
  }
  assert.deepEqual(offenders, [], "inline handlers never run under the page's policy");
  assert(!/\son[a-z]+=/.test(avatarHtml("https://x/p.png", "a".repeat(64))));
  ok(`no module under web/ emits an inline event handler (${files.length} read)`);
}

console.log("broken: dead pictures are undone by data-broken and one delegated listener");
