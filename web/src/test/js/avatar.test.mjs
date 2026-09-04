// The face renderer against the size table in index.html: a size the
// stylesheet has no row for renders wrong or not at all, and nothing throws.

import assert from "assert";
import { readFileSync } from "node:fs";

const { avatarHtml, SIZES, hueOf, BLANK } =
  await import(new URL("../../main/resources/web/shared/avatar.js", import.meta.url));
const html = readFileSync(new URL("../../main/resources/index.html", import.meta.url), "utf8");

const pk = "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2";

for (const size of SIZES) {
  const rule = new RegExp(`\\.av-${size}\\s*{[^}]*--av:`);
  assert.ok(rule.test(html), `index.html has no --av rule for .av-${size}`);
  assert.ok(avatarHtml("", pk, size).includes(`av-wrap av-${size}`), `avatarHtml did not emit av-${size}`);
}

assert.throws(() => avatarHtml("", pk, "medium"), /unknown avatar size/);

const withPic = avatarHtml("https://example.test/p.png?a=1&b=2", pk, "lg");
assert.ok(withPic.includes(`src="https://example.test/p.png?a=1&amp;b=2"`), "the picture url must be escaped");
assert.ok(withPic.includes('referrerpolicy="no-referrer"'), "a face must not leak the reader to the picture's host");
assert.ok(withPic.includes('loading="lazy"'));
assert.ok(withPic.includes(BLANK), "a broken picture must fall back in place");
assert.ok(withPic.includes(`class="score-chip" data-pk="${pk}"`), "every face carries a score chip");

const noPic = avatarHtml("", pk, "sm");
assert.ok(noPic.includes('class="avatar gen"'), "a pictureless person still gets a face");
assert.ok(!noPic.includes("<img"), "no <img> when there is nothing to load");
assert.ok(noPic.includes(`--h:${hueOf(pk)}`), "the generated face is keyed off the pubkey");

for (const seed of [pk, "0000" + "a".repeat(60), "ffff" + "0".repeat(60), "", null]) {
  const h = hueOf(seed);
  assert.ok(Number.isInteger(h) && h >= 0 && h < 360, `hueOf(${seed}) = ${h}`);
  assert.strictEqual(h, hueOf(seed), "the same person must always draw the same face");
}

// paintScores() queries on the data-pk attribute; an empty seed still needs a well-formed chip.
assert.ok(avatarHtml("", "", "lg").includes('data-pk=""'));

console.log("avatar: one renderer, sizes matched to the stylesheet's table");
