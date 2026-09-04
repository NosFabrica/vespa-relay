// NIP-19 identifiers, both directions, with no library. Encoding emits npub,
// note, naddr and nevent; decoding accepts all five forms, because nprofile
// arrives in pasted links whether or not this page would have minted it.
// Anywhere a person sees a key it is an npub, never hex.

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

function bech32Bytes(hrp, bytes) {
  const words = toWords(bytes);
  const chk = polymod(expand(hrp).concat(words, [0, 0, 0, 0, 0, 0])) ^ 1;
  const sum = Array.from({ length: 6 }, (_, i) => (chk >> (5 * (5 - i))) & 31);
  return hrp + "1" + words.concat(sum).map((w) => B32[w]).join("");
}

const hexToBytes = (hex) => hex.match(/../g).map((h) => parseInt(h, 16));

/**
 * Empty for anything that is not a 32-byte hex id, rather than a throw: cards
 * render events from strangers' relays, and a missing id should cost one link,
 * not the page.
 */
const bech32 = (hrp, hex) => (/^[0-9a-f]{64}$/i.test(hex || "") ? bech32Bytes(hrp, hexToBytes(String(hex).toLowerCase())) : "");

/**
 * Decode and verify; null for anything malformed, including a bad checksum.
 * No 90-character cap: NIP-19 says to ignore bech32's limit, and identifiers
 * with relay hints routinely exceed it.
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
/** The npub for one narrow line, prefix only: `npub1eedm57z…`. */
export const tinyNpub = (hex) => npub(hex).slice(0, 12) + "…";
export const shortNote = (hex) => shortB32(noteId(hex));

/**
 * The `kind:pubkey:d` coordinate of a parameterized replaceable event, or
 * null. An absent `d` is the empty string, which is a legal address; a pubkey
 * that is not 64 hex is not, since an naddr minted from it links nowhere. Kept
 * beside the encoder it feeds so the card and the provenance pill spell one
 * address the same way.
 */
export function addrOf(ev) {
  if (!ev || !Number.isInteger(ev.kind) || ev.kind < 30000 || ev.kind > 39999) return null;
  if (!/^[0-9a-f]{64}$/.test(ev.pubkey || "")) return null;
  const d = ((ev.tags || []).find((t) => Array.isArray(t) && t[0] === "d" && typeof t[1] === "string") || [])[1] || "";
  return `${ev.kind}:${ev.pubkey}:${d}`;
}

/**
 * An `a` tag's `kind:pubkey:d` as an naddr, or null when the tag is malformed,
 * so the card renders the entry as text instead of linking to a page that
 * never resolves. The TLV length is one byte, so a `d` over 255 UTF-8 bytes
 * has no legal encoding.
 */
export function naddr(a) {
  const m = /^(\d+):([0-9a-f]{64}):([\s\S]*)$/.exec(String(a || ""));
  if (!m) return null;
  const kind = Number(m[1]);
  if (!Number.isInteger(kind) || kind < 0 || kind > 0xffffffff) return null;
  const d = [...new TextEncoder().encode(m[3])];
  if (d.length > 255) return null;
  return bech32Bytes("naddr", [
    0, d.length, ...d,                                    // 0: the d identifier
    2, 32, ...hexToBytes(m[2]),                            // 2: the author
    3, 4, (kind >>> 24) & 255, (kind >>> 16) & 255, (kind >>> 8) & 255, kind & 255, // 3: the kind
  ]);
}

/**
 * An event id plus whatever hints are known, as an nevent, or "" for a bad id.
 * Only what is known is encoded; a caller with no hints should use noteId(),
 * and cards/base.js's eventHref picks for them. Relays are capped at two
 * because this ends up in an address bar.
 */
export function nevent(id, { relays = [], author = null, kind = null } = {}) {
  if (!/^[0-9a-f]{64}$/i.test(id || "")) return "";
  const bytes = [0, 32, ...hexToBytes(String(id).toLowerCase())];
  for (const r of relays.slice(0, 2)) {
    const b = [...new TextEncoder().encode(String(r || ""))];
    if (b.length && b.length <= 255) bytes.push(1, b.length, ...b);
  }
  if (/^[0-9a-f]{64}$/i.test(author || "")) bytes.push(2, 32, ...hexToBytes(String(author).toLowerCase()));
  // `kind == null` first: kind 0 is a real kind and `Number(null)` is 0.
  const k = kind == null ? NaN : Number(kind);
  if (Number.isInteger(k) && k >= 0 && k <= 0xffffffff) {
    bytes.push(3, 4, (k >>> 24) & 255, (k >>> 16) & 255, (k >>> 8) & 255, k & 255);
  }
  return bech32Bytes("nevent", bytes);
}

/** The half of an `a` tag worth showing: its `d`, else the address itself. */
export const shortAddr = (a) => {
  const m = /^(\d+):([0-9a-f]{64}):([\s\S]*)$/.exec(String(a || ""));
  return m ? (m[3] || shortNpub(m[2])) : String(a || "");
};

/**
 * An npub or bare hex back to a hex pubkey; null if malformed, so a corrupted
 * `as=` parameter degrades to "ranking as you" rather than a lens that ranks
 * nothing.
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
 * All carry `raw` and the TLV forms carry `relays`, which entity.js gates
 * before dialing.
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
  // TLV: type byte, length byte, value; truncation mid-entry is malformed.
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
  // naddr: the d identifier may legitimately be empty, so only author and
  // kind are required.
  const dTag = one(0);
  const author = one(2);
  const kind = one(3);
  if (!author || author.length !== 32 || !kind || be32(kind) === null) return null;
  return { type: "naddr", kind: be32(kind), author: bytesToHex(author), d: utf8(dTag || []), relays, raw: v };
}

const utf8 = (bytes) => new TextDecoder().decode(new Uint8Array(bytes));
