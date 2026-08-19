// The one face renderer, and the CSS table it names sizes out of.
//
// The interesting failure is silent: a size the stylesheet has no row for
// leaves `--av` at its inherited value (or nothing), so the face renders at
// the wrong size — or at none — and nothing throws. So the vocabulary in
// shared/avatar.js is checked against the rules in index.html here, the same
// way preload.test.mjs checks the module hints against the real import graph.

import assert from "assert";
import { readFileSync } from "node:fs";

const { avatarHtml, SIZES, hueOf, BLANK } =
  await import(new URL("../../web/src/main/resources/web/shared/avatar.js", import.meta.url));
const html = readFileSync(new URL("../../web/src/main/resources/index.html", import.meta.url), "utf8");

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";

// ---- every size the module offers is a size the stylesheet sizes ----------
for (const size of SIZES) {
  const rule = new RegExp(`\\.av-${size}\\s*{[^}]*--av:`);
  assert.ok(rule.test(html), `index.html has no --av rule for .av-${size}`);
  assert.ok(avatarHtml("", pk, size).includes(`av-wrap av-${size}`), `avatarHtml did not emit av-${size}`);
}

// A typo must fail loudly rather than draw a face with no width.
assert.throws(() => avatarHtml("", pk, "medium"), /unknown avatar size/);

// ---- what a face is made of ----------------------------------------------
const withPic = avatarHtml("https://example.test/p.png?a=1&b=2", pk, "lg");
assert.ok(withPic.includes(`src="https://example.test/p.png?a=1&amp;b=2"`), "the picture url must be escaped");
assert.ok(withPic.includes('referrerpolicy="no-referrer"'), "a face must not leak the reader to the picture's host");
assert.ok(withPic.includes('loading="lazy"'));
assert.ok(withPic.includes(BLANK), "a broken picture must fall back in place");
assert.ok(withPic.includes(`class="score-chip" data-pk="${pk}"`), "every face carries a score chip");

// No picture is not a broken picture: it is the generated face, straight away.
const noPic = avatarHtml("", pk, "sm");
assert.ok(noPic.includes('class="avatar gen"'), "a pictureless person still gets a face");
assert.ok(!noPic.includes("<img"), "no <img> when there is nothing to load");
assert.ok(noPic.includes(`--h:${hueOf(pk)}`), "the generated face is keyed off the pubkey");

// The hue is stable per pubkey and stays inside a circle of degrees.
for (const seed of [pk, "0000" + "a".repeat(60), "ffff" + "0".repeat(60), "", null]) {
  const h = hueOf(seed);
  assert.ok(Number.isInteger(h) && h >= 0 && h < 360, `hueOf(${seed}) = ${h}`);
  assert.strictEqual(h, hueOf(seed), "the same person must always draw the same face");
}

// A pubkey-shaped attribute is what paintScores() queries on; an empty seed
// must still produce a well-formed (if unscorable) chip rather than markup.
assert.ok(avatarHtml("", "", "lg").includes('data-pk=""'));

console.log("avatar: one renderer, sizes matched to the stylesheet's table");
