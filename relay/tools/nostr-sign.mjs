// BIP-340 Schnorr over BigInt and the NIP-01 event it signs, for the tools beside it that publish:
// the reference algorithm, slow (tens of milliseconds a signature) and dependency-free on purpose.
import { createHash, randomBytes } from "node:crypto";

const P = 2n ** 256n - 2n ** 32n - 977n;
const N = 0xfffffffffffffffffffffffffffffffebaaedce6af48a03bbfd25e8cd0364141n;
const G = [0x79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798n, 0x483ada7726a3c4655da4fbfc0e1108a8fd17b448a68554199c47d08ffb10d4b8n];
const mod = (a, m = P) => ((a % m) + m) % m;
const pow = (b, e, m) => { let r = 1n; b = mod(b, m); while (e > 0n) { if (e & 1n) r = (r * b) % m; b = (b * b) % m; e >>= 1n; } return r; };
const inv = (a) => pow(a, P - 2n, P);
const add = (p1, p2) => {
  if (!p1) return p2; if (!p2) return p1;
  if (p1[0] === p2[0] && p1[1] !== p2[1]) return null;
  const l = p1[0] === p2[0] ? mod(3n * p1[0] * p1[0] * inv(2n * p1[1])) : mod((p2[1] - p1[1]) * inv(p2[0] - p1[0]));
  const x = mod(l * l - p1[0] - p2[0]);
  return [x, mod(l * (p1[0] - x) - p1[1])];
};
const mul = (k, pt = G) => { let r = null; while (k > 0n) { if (k & 1n) r = add(r, pt); pt = add(pt, pt); k >>= 1n; } return r; };
const hex = (b) => Buffer.from(b).toString("hex");
const b32 = (n) => Buffer.from(n.toString(16).padStart(64, "0"), "hex");
const int = (b) => BigInt("0x" + hex(b));
const sha = (...parts) => createHash("sha256").update(Buffer.concat(parts.map((p) => Buffer.from(p)))).digest();
const tagged = (tag, ...m) => { const t = sha(Buffer.from(tag)); return sha(t, t, ...m); };
export const keypair = () => { const d = mod(int(randomBytes(32)), N - 1n) + 1n; return { d, pub: hex(b32(mul(d)[0])) }; };
const sign = (msg, d0) => {
  const Pt = mul(d0); const d = Pt[1] % 2n === 0n ? d0 : N - d0;
  const t = Buffer.from(b32(d).map((x, i) => x ^ tagged("BIP0340/aux", randomBytes(32))[i]));
  const k0 = mod(int(tagged("BIP0340/nonce", t, b32(Pt[0]), msg)), N);
  const R = mul(k0); const k = R[1] % 2n === 0n ? k0 : N - k0;
  const e = mod(int(tagged("BIP0340/challenge", b32(R[0]), b32(Pt[0]), msg)), N);
  return hex(Buffer.concat([b32(R[0]), b32(mod(k + e * d, N))]));
};
export const event = (key, kind, tags, content, created_at = Math.floor(Date.now() / 1000)) => {
  const id = hex(sha(JSON.stringify([0, key.pub, created_at, kind, tags, content])));
  return { id, pubkey: key.pub, created_at, kind, tags, content, sig: sign(Buffer.from(id, "hex"), key.d) };
};
export const nip98 = (key, url, body) =>
  "Nostr " + Buffer.from(JSON.stringify(event(key, 27235, [["u", url], ["method", "POST"], ["payload", hex(sha(body))]], ""))).toString("base64");
