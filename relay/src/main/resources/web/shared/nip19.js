// NIP-19 identifiers, both directions, hand-rolled — no library. npub, never
// hex, anywhere a person sees the value: a 64-char hash is not an identifier
// a person picks from a list, and it is not what any other Nostr client would
// show them. `note` is the bare event id; nevent/naddr carry TLV hints this
// page has nothing to put in, so the plain forms are the honest ones.

const B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";

function bech32(hrp, hex) {
  const bytes = hex.match(/../g).map((h) => parseInt(h, 16));
  // 8-bit -> 5-bit regrouping, then bech32's checksum over hrp + payload.
  const words = [];
  let acc = 0, bits = 0;
  for (const b of bytes) {
    acc = (acc << 8) | b; bits += 8;
    while (bits >= 5) { bits -= 5; words.push((acc >> bits) & 31); }
  }
  if (bits) words.push((acc << (5 - bits)) & 31);
  const polymod = (v) => {
    const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];
    let chk = 1;
    for (const x of v) {
      const top = chk >> 25;
      chk = ((chk & 0x1ffffff) << 5) ^ x;
      for (let i = 0; i < 5; i++) if ((top >> i) & 1) chk ^= GEN[i];
    }
    return chk;
  };
  const expand = [...hrp].map((c) => c.charCodeAt(0) >> 5).concat([0], [...hrp].map((c) => c.charCodeAt(0) & 31));
  const chk = polymod(expand.concat(words, [0, 0, 0, 0, 0, 0])) ^ 1;
  const sum = Array.from({ length: 6 }, (_, i) => (chk >> (5 * (5 - i))) & 31);
  return hrp + "1" + words.concat(sum).map((w) => B32[w]).join("");
}

export const npub = (hex) => bech32("npub", hex);
export const noteId = (hex) => bech32("note", hex);
const shortB32 = (v) => v.slice(0, 12) + "…" + v.slice(-6);
export const shortNpub = (hex) => shortB32(npub(hex));
export const shortNote = (hex) => shortB32(noteId(hex));

/**
 * The inverse, for reading `as=` out of the URL: an npub back to hex. Bare
 * hex is accepted too, because a URL is typed and pasted by hand. The
 * checksum is verified by re-encoding through the one encoder above rather
 * than by a second polymod path — a corrupted npub becomes null (ignored, so
 * the page falls back to ranking as you) instead of a lens that silently
 * ranks nothing.
 */
export function pubkeyParam(v) {
  v = String(v || "").trim().toLowerCase();
  if (/^[0-9a-f]{64}$/.test(v)) return v;
  if (!/^npub1[a-z0-9]{58}$/.test(v)) return null;
  const words = [...v.slice(5)].map((c) => B32.indexOf(c));
  if (words.includes(-1)) return null;
  let acc = 0, bits = 0;
  const bytes = [];
  for (const w of words.slice(0, -6)) {
    acc = (acc << 5) | w; bits += 5;
    if (bits >= 8) { bits -= 8; bytes.push((acc >> bits) & 255); }
  }
  if (bytes.length !== 32) return null;
  const hex = bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
  return npub(hex) === v ? hex : null;
}
