import assert from 'assert';
const { npub, noteId, naddr: mintAddr, nevent: mintEvent, shortAddr, pubkeyParam, nip19Parse } =
  await import(new URL("../../main/resources/web/shared/nip19.js", import.meta.url));

// A test-side bech32+TLV encoder, independent of the page's decoder so the two check each other.
const B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
const polymod = (v) => { let c = 1; for (const x of v) { const t = c >> 25; c = ((c & 0x1ffffff) << 5) ^ x; for (let i = 0; i < 5; i++) if ((t >> i) & 1) c ^= GEN[i]; } return c; };
const expand = (h) => [...h].map(c => c.charCodeAt(0) >> 5).concat([0], [...h].map(c => c.charCodeAt(0) & 31));
function encode(hrp, bytes) {
  const words = [];
  let acc = 0, bits = 0;
  for (const b of bytes) { acc = (acc << 8) | b; bits += 8; while (bits >= 5) { bits -= 5; words.push((acc >> bits) & 31); } }
  if (bits) words.push((acc << (5 - bits)) & 31);
  const chk = polymod(expand(hrp).concat(words, [0,0,0,0,0,0])) ^ 1;
  return hrp + "1" + words.concat(Array.from({length:6},(_,i)=>(chk>>(5*(5-i)))&31)).map(w=>B32[w]).join("");
}
const hexBytes = (h) => h.match(/../g).map(x => parseInt(x, 16));
const tlv = (entries) => entries.flatMap(([t, v]) => [t, v.length, ...v]);
const utf8 = (s) => [...new TextEncoder().encode(s)];

const pk = "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d";
const id = "b".repeat(63) + "1"; // hex chars only
const idHex = "abcdef0123456789".repeat(4);

assert.deepStrictEqual(nip19Parse(npub(pk)), { type: "npub", pubkey: pk, raw: npub(pk) });
assert.deepStrictEqual(nip19Parse(noteId(idHex)), { type: "note", id: idHex, raw: noteId(idHex) });
assert.strictEqual(nip19Parse("nostr:" + npub(pk)).pubkey, pk, "nostr: prefix stripped");

const nprofile = encode("nprofile", tlv([[0, hexBytes(pk)], [1, utf8("wss://r.x.com")], [1, utf8("wss://djbas.sadkb.com")]]));
let p = nip19Parse(nprofile);
assert.strictEqual(p.type, "nprofile");
assert.strictEqual(p.pubkey, pk);
assert.deepStrictEqual(p.relays, ["wss://r.x.com", "wss://djbas.sadkb.com"]);

const nevent = encode("nevent", tlv([[0, hexBytes(idHex)], [1, utf8("wss://relay.example.com")], [2, hexBytes(pk)], [3, [0, 0, 0x75, 0x4f]]]));
assert(nevent.length > 90, "nevent with hints exceeds classic bech32 length");
p = nip19Parse(nevent);
assert.strictEqual(p.type, "nevent");
assert.strictEqual(p.id, idHex);
assert.strictEqual(p.author, pk);
assert.strictEqual(p.kind, 30031);

const naddr = encode("naddr", tlv([[0, utf8("my-article")], [2, hexBytes(pk)], [3, [0, 0, 0x75, 0x57]]]));
p = nip19Parse(naddr);
assert.deepStrictEqual({ type: p.type, kind: p.kind, author: p.author, d: p.d }, { type: "naddr", kind: 30039, author: pk, d: "my-article" });

// An empty d is legal.
p = nip19Parse(encode("naddr", tlv([[0, []], [2, hexBytes(pk)], [3, [0, 0, 0, 3]]])));
assert.strictEqual(p.d, "");
assert.strictEqual(p.kind, 3);

const corrupt = nprofile.slice(0, -1) + (nprofile.endsWith("q") ? "p" : "q");
assert.strictEqual(nip19Parse(corrupt), null, "checksum catches corruption");
assert.strictEqual(nip19Parse(encode("nprofile", [0, 40, 1, 2, 3])), null, "overrunning TLV length is malformed");
assert.strictEqual(nip19Parse(encode("naddr", tlv([[0, utf8("d")], [3, [0,0,0,3]]]))), null);
assert.strictEqual(nip19Parse(encode("nsec", hexBytes(pk))), null, "nsec is not a page");
assert.strictEqual(nip19Parse(""), null);
assert.strictEqual(nip19Parse("npub1"), null);

// A minted naddr must survive the page's own decoder.
for (const [tag, kind, d] of [
  [`30023:${pk}:my-article`, 30023, "my-article"],
  [`3:${pk}:`, 3, ""],                                   // a d-less replaceable
  [`30030:${pk}:emoji ünïcode`, 30030, "emoji ünïcode"], // multibyte d
  [`0:${pk}:`, 0, ""],
]) {
  const minted = mintAddr(tag);
  p = nip19Parse(minted);
  assert.deepStrictEqual({ type: p.type, kind: p.kind, author: p.author, d: p.d },
    { type: "naddr", kind, author: pk, d }, `naddr round trip for ${tag}`);
}
// A malformed `a` tag mints nothing; the card falls back to plain text.
assert.strictEqual(mintAddr("not-an-address"), null);
assert.strictEqual(mintAddr(`30023:${pk.slice(0, 63)}:x`), null, "short pubkey");
assert.strictEqual(mintAddr(`30023:${pk.toUpperCase()}:x`), null, "hex is lowercase");
assert.strictEqual(mintAddr(""), null);
assert.strictEqual(mintAddr(null), null);
// The TLV length prefix is one byte, counted in utf-8 bytes.
assert.strictEqual(mintAddr(`30023:${pk}:${"x".repeat(256)}`), null, "256-byte d has no legal TLV");
assert.strictEqual(typeof mintAddr(`30023:${pk}:${"x".repeat(255)}`), "string", "255 bytes still fits");
assert.strictEqual(mintAddr(`30023:${pk}:${"ü".repeat(200)}`), null, "the limit counts utf-8 bytes");

// The hints are why a parent link is an nevent rather than a note, so the round trip keeps them.
p = nip19Parse(mintEvent(idHex, { relays: ["wss://hint.example"], author: pk, kind: 1 }));
assert.deepStrictEqual({ type: p.type, id: p.id, author: p.author, kind: p.kind, relays: p.relays },
  { type: "nevent", id: idHex, author: pk, kind: 1, relays: ["wss://hint.example"] }, "nevent round trip");
p = nip19Parse(mintEvent(idHex, { author: pk }));
assert.deepStrictEqual([p.id, p.author, p.relays, p.kind], [idHex, pk, [], null], "an nevent with only an author");
p = nip19Parse(mintEvent(idHex));
assert.deepStrictEqual([p.id, p.author, p.relays], [idHex, null, []], "a hintless nevent is just the id");
assert.deepStrictEqual(nip19Parse(mintEvent(idHex, { relays: ["wss://a.x", "wss://b.x", "wss://c.x"] })).relays,
  ["wss://a.x", "wss://b.x"], "relay hints are capped at two");
assert.strictEqual(mintEvent("nope"), "");
assert.strictEqual(mintEvent(null), "");
p = nip19Parse(mintEvent(idHex, { author: "not-a-pubkey", kind: "soon" }));
assert.deepStrictEqual([p.id, p.author, p.kind], [idHex, null, null], "junk hints are dropped, the id survives");

assert.strictEqual(shortAddr(`30023:${pk}:my-article`), "my-article");
assert.strictEqual(shortAddr(`3:${pk}:`), npub(pk).slice(0, 12) + "…" + npub(pk).slice(-6));
assert.strictEqual(shortAddr("garbage"), "garbage");

assert.strictEqual(pubkeyParam(npub(pk)), pk);
assert.strictEqual(pubkeyParam(pk), pk);
assert.strictEqual(pubkeyParam(npub(pk).toUpperCase()), pk);
assert.strictEqual(pubkeyParam("npub1qqqq"), null);

console.log("nip19: all assertions passed");
