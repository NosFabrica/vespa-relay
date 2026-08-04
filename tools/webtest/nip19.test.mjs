import assert from 'assert';
const { npub, noteId, pubkeyParam, nip19Parse } =
  await import(new URL("../../relay/src/main/resources/web/shared/nip19.js", import.meta.url));

// A test-side bech32+TLV ENCODER, written independently from the page's
// decoder so the two paths check each other.
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

// plain forms round-trip against the page's own encoder
assert.deepStrictEqual(nip19Parse(npub(pk)), { type: "npub", pubkey: pk, raw: npub(pk) });
assert.deepStrictEqual(nip19Parse(noteId(idHex)), { type: "note", id: idHex, raw: noteId(idHex) });
assert.strictEqual(nip19Parse("nostr:" + npub(pk)).pubkey, pk, "nostr: prefix stripped");

// nprofile: pubkey + two relay hints
const nprofile = encode("nprofile", tlv([[0, hexBytes(pk)], [1, utf8("wss://r.x.com")], [1, utf8("wss://djbas.sadkb.com")]]));
let p = nip19Parse(nprofile);
assert.strictEqual(p.type, "nprofile");
assert.strictEqual(p.pubkey, pk);
assert.deepStrictEqual(p.relays, ["wss://r.x.com", "wss://djbas.sadkb.com"]);

// nevent: id + author + kind, exceeding 90 chars total
const nevent = encode("nevent", tlv([[0, hexBytes(idHex)], [1, utf8("wss://relay.example.com")], [2, hexBytes(pk)], [3, [0, 0, 0x75, 0x4f]]]));
assert(nevent.length > 90, "nevent with hints exceeds classic bech32 length");
p = nip19Parse(nevent);
assert.strictEqual(p.type, "nevent");
assert.strictEqual(p.id, idHex);
assert.strictEqual(p.author, pk);
assert.strictEqual(p.kind, 30031);

// naddr: d + author + kind
const naddr = encode("naddr", tlv([[0, utf8("my-article")], [2, hexBytes(pk)], [3, [0, 0, 0x75, 0x57]]]));
p = nip19Parse(naddr);
assert.deepStrictEqual({ type: p.type, kind: p.kind, author: p.author, d: p.d }, { type: "naddr", kind: 30039, author: pk, d: "my-article" });

// naddr with an EMPTY d (legal) still parses
p = nip19Parse(encode("naddr", tlv([[0, []], [2, hexBytes(pk)], [3, [0, 0, 0, 3]]])));
assert.strictEqual(p.d, "");
assert.strictEqual(p.kind, 3);

// corruption: flip one char -> null, not a wrong answer
const corrupt = nprofile.slice(0, -1) + (nprofile.endsWith("q") ? "p" : "q");
assert.strictEqual(nip19Parse(corrupt), null, "checksum catches corruption");
// truncated TLV entry -> null
assert.strictEqual(nip19Parse(encode("nprofile", [0, 40, 1, 2, 3])), null, "overrunning TLV length is malformed");
// naddr missing its author -> null
assert.strictEqual(nip19Parse(encode("naddr", tlv([[0, utf8("d")], [3, [0,0,0,3]]]))), null);
// nsec must never parse
assert.strictEqual(nip19Parse(encode("nsec", hexBytes(pk))), null, "nsec is not a page");
assert.strictEqual(nip19Parse(""), null);
assert.strictEqual(nip19Parse("npub1"), null);

// pubkeyParam still behaves (regression from the rewrite)
assert.strictEqual(pubkeyParam(npub(pk)), pk);
assert.strictEqual(pubkeyParam(pk), pk);
assert.strictEqual(pubkeyParam(npub(pk).toUpperCase()), pk);
assert.strictEqual(pubkeyParam("npub1qqqq"), null);

console.log("nip19: all assertions passed");
