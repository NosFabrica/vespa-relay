// NIP-19 identifiers, both directions, hand-rolled — no library. npub, never
// hex, anywhere a person sees the value: a 64-char hash is not an identifier
// a person picks from a list, and it is not what any other Nostr client would
// show them. Encoding emits only the plain forms (npub/note) — nevent/naddr
// carry TLV hints this page has nothing to put in — but DECODING accepts all
// five, because /nevent1… and /naddr1… arrive in pasted links whether or not
// we would have minted them.

const B32 = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
const GEN = [0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3];

function polymod(v) {
  let chk = 1;
  for (const x of v) {
    const top = chk >> 25;
    chk = ((chk & 0x1ffffff) << 5) ^ x;
    for (let i = 0; i < 5; i++) if ((top >> i) & 1) chk ^= GEN[i];
  }
  return chk;
}

const expand = (hrp) => [...hrp].map((c) => c.charCodeAt(0) >> 5).concat([0], [...hrp].map((c) => c.charCodeAt(0) & 31));

function toWords(bytes) {
  const words = [];
  let acc = 0, bits = 0;
  for (const b of bytes) {
    acc = (acc << 8) | b; bits += 8;
    while (bits >= 5) { bits -= 5; words.push((acc >> bits) & 31); }
  }
  if (bits) words.push((acc << (5 - bits)) & 31);
  return words;
}

function fromWords(words) {
  const bytes = [];
  let acc = 0, bits = 0;
  for (const w of words) {
    acc = (acc << 5) | w; bits += 5;
    if (bits >= 8) { bits -= 8; bytes.push((acc >> bits) & 255); }
  }
  return bytes;
}

function bech32(hrp, hex) {
  const bytes = hex.match(/../g).map((h) => parseInt(h, 16));
  const words = toWords(bytes);
  const chk = polymod(expand(hrp).concat(words, [0, 0, 0, 0, 0, 0])) ^ 1;
  const sum = Array.from({ length: 6 }, (_, i) => (chk >> (5 * (5 - i))) & 31);
  return hrp + "1" + words.concat(sum).map((w) => B32[w]).join("");
}

/**
 * Decode + verify. Null for anything malformed, including a bad checksum —
 * a corrupted identifier must become "invalid", never a plausible-looking
 * pubkey that silently names nobody.
 *
 * Deliberately NO 90-character cap: classic bech32 has one, and NIP-19
 * identifiers with relay hints routinely exceed it — the NIP says to ignore
 * the limit, and enforcing it would reject most nevents in the wild.
 */
function bech32Decode(str) {
  const pos = str.lastIndexOf("1");
  if (pos < 1 || str.length < pos + 7) return null;
  const hrp = str.slice(0, pos);
  const words = [...str.slice(pos + 1)].map((c) => B32.indexOf(c));
  if (words.includes(-1)) return null;
  if (polymod(expand(hrp).concat(words)) !== 1) return null;
  return { hrp, bytes: fromWords(words.slice(0, -6)) };
}

const bytesToHex = (b) => b.map((x) => x.toString(16).padStart(2, "0")).join("");

export const npub = (hex) => bech32("npub", hex);
export const noteId = (hex) => bech32("note", hex);
const shortB32 = (v) => v.slice(0, 12) + "…" + v.slice(-6);
export const shortNpub = (hex) => shortB32(npub(hex));
export const shortNote = (hex) => shortB32(noteId(hex));

/**
 * An npub (or bare hex, because URLs are typed and pasted by hand) back to a
 * hex pubkey; null if malformed. Used for the `as=` URL parameter, where a
 * corrupted value degrades to "ranking as you" rather than a lens that
 * silently ranks nothing.
 */
export function pubkeyParam(v) {
  v = String(v || "").trim().toLowerCase();
  if (/^[0-9a-f]{64}$/.test(v)) return v;
  if (!v.startsWith("npub1")) return null;
  const d = bech32Decode(v);
  return d && d.hrp === "npub" && d.bytes.length === 32 ? bytesToHex(d.bytes) : null;
}

/**
 * Any NIP-19 identifier a URL path can carry, parsed to what it names:
 *
 *   npub / nprofile -> { type, pubkey }            a person
 *   note / nevent   -> { type, id, kind?, author? } one event by id
 *   naddr           -> { type, kind, author, d }    one replaceable address
 *
 * All carry `raw` (the identifier as given, for links that must reproduce
 * it) and TLV forms carry `relays` — parsed but currently unused: this page
 * reads only the relay that serves it, and dialing arbitrary hint relays
 * from a pasted link is a different client than this one means to be.
 */
export function nip19Parse(input) {
  const v = String(input || "").trim().replace(/^nostr:/i, "").toLowerCase();
  if (!/^(npub|nprofile|note|nevent|naddr)1[a-z0-9]+$/.test(v)) return null;
  const d = bech32Decode(v);
  if (!d) return null;
  const { hrp, bytes } = d;
  if (hrp === "npub" || hrp === "note") {
    if (bytes.length !== 32) return null;
    const hex = bytesToHex(bytes);
    return hrp === "npub" ? { type: "npub", pubkey: hex, raw: v } : { type: "note", id: hex, raw: v };
  }
  // TLV: type byte, length byte, value — truncation mid-entry is malformed.
  const tlv = [];
  for (let i = 0; i < bytes.length;) {
    const t = bytes[i], l = bytes[i + 1];
    if (l === undefined || i + 2 + l > bytes.length) return null;
    tlv.push({ t, v: bytes.slice(i + 2, i + 2 + l) });
    i += 2 + l;
  }
  const one = (t) => tlv.find((e) => e.t === t)?.v;
  const relays = tlv.filter((e) => e.t === 1).map((e) => utf8(e.v));
  const be32 = (b) => b.length === 4 ? ((b[0] << 24) | (b[1] << 16) | (b[2] << 8) | b[3]) >>> 0 : null;
  if (hrp === "nprofile") {
    const pk = one(0);
    if (!pk || pk.length !== 32) return null;
    return { type: "nprofile", pubkey: bytesToHex(pk), relays, raw: v };
  }
  if (hrp === "nevent") {
    const id = one(0);
    if (!id || id.length !== 32) return null;
    const author = one(2);
    const kind = one(3);
    return {
      type: "nevent", id: bytesToHex(id), relays, raw: v,
      author: author && author.length === 32 ? bytesToHex(author) : null,
      kind: kind ? be32(kind) : null,
    };
  }
  // naddr: the d identifier may legitimately be EMPTY (a d-less replaceable),
  // so only author and kind are hard requirements.
  const dTag = one(0);
  const author = one(2);
  const kind = one(3);
  if (!author || author.length !== 32 || !kind || be32(kind) === null) return null;
  return { type: "naddr", kind: be32(kind), author: bytesToHex(author), d: utf8(dTag || []), relays, raw: v };
}

const utf8 = (bytes) => new TextDecoder().decode(new Uint8Array(bytes));
