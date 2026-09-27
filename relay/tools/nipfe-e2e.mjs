// Drives NIP-FE end to end against a RUNNING relay -- one client frame POSTed to the relay's URL, the
// socket's own frames back -- and checks each answer against the same frame sent on its websocket.
// The spec is the nostrhub text (kind 30817, d=nip-fe-nostr-over-http); docs/proposals/ carries a copy.
//
// Node 21+, no dependencies, same reasons as `fetch-corpus.mjs` beside it. It is a shell tool, not a
// test: it needs a relay up (docker compose) over a real store, and the parity it checks only means
// something over real data. Load one first:
//
//   node fetch-corpus.mjs /tmp/corpus
//   ./gradlew :relay:test --tests '*ProductionCorpusIT*' -DitVespa=http://localhost:8080 -DitCorpus=/tmp/corpus
//   docker compose up -d relay
//   node nipfe-e2e.mjs http://localhost:7777
//
// What it covers: the lens gate's 401; REQ over production filters and full-text searches, the same
// frames as the socket, subscription id and order included; COUNT; gzip on the wire; NIP-98 (signed in
// without a lens, not single-use, bound to its body and the relay's url, other schemes ignored); EVENT
// (OK, served after, duplicate, forged, stale replaceable); every refusal before a command runs (a
// NOTICE); a deep body; NIP-86's Content-Type on the same URL; and the per-client gate under 40
// concurrent requests, which expects the default HTTP_RELAY_PER_CLIENT.
//
// It WRITES: one signed note and two kind-0s under throwaway keys, plus the signature self-check.
// Point it at a local relay, never at staging or production (AGENTS.md: read staging, never publish).
// BIP-340 Schnorr is the reference algorithm over BigInt, for the NIP-98 tokens and those events; the
// first check is that the relay accepts its signature, so a bad signer cannot pass for a relay bug.
// OBSERVER=<64-hex> ranks the lensed searches through another key.
import { createHash, randomBytes } from "node:crypto";
import http from "node:http";
import { gunzipSync } from "node:zlib";

const BASE = process.argv[2] || "http://localhost:7777";
const WS = BASE.replace(/^http/, "ws");
const RANKER = process.env.OBSERVER || "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c";

// ---------------------------------------------------------------- secp256k1 / BIP-340
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
const keypair = () => { const d = mod(int(randomBytes(32)), N - 1n) + 1n; return { d, pub: hex(b32(mul(d)[0])) }; };
const sign = (msg, d0) => {
  const Pt = mul(d0); const d = Pt[1] % 2n === 0n ? d0 : N - d0;
  const t = Buffer.from(b32(d).map((x, i) => x ^ tagged("BIP0340/aux", randomBytes(32))[i]));
  const k0 = mod(int(tagged("BIP0340/nonce", t, b32(Pt[0]), msg)), N);
  const R = mul(k0); const k = R[1] % 2n === 0n ? k0 : N - k0;
  const e = mod(int(tagged("BIP0340/challenge", b32(R[0]), b32(Pt[0]), msg)), N);
  return hex(Buffer.concat([b32(R[0]), b32(mod(k + e * d, N))]));
};
const event = (key, kind, tags, content, created_at = Math.floor(Date.now() / 1000)) => {
  const id = hex(sha(JSON.stringify([0, key.pub, created_at, kind, tags, content])));
  return { id, pubkey: key.pub, created_at, kind, tags, content, sig: sign(Buffer.from(id, "hex"), key.d) };
};
const nip98 = (key, url, body) =>
  "Nostr " + Buffer.from(JSON.stringify(event(key, 27235, [["u", url], ["method", "POST"], ["payload", hex(sha(body))]], ""))).toString("base64");

// ---------------------------------------------------------------- transports
const URL_ = BASE.endsWith("/") ? BASE : BASE + "/";
async function post(body, headers = {}) {
  const r = await fetch(URL_, { method: "POST", body, headers: { "accept-encoding": "identity", ...headers } });
  const raw = Buffer.from(await r.arrayBuffer());
  const text = (r.headers.get("content-encoding") === "gzip" ? gunzipSync(raw) : raw).toString();
  return { status: r.status, headers: r.headers, lines: text.split("\n").filter(Boolean) };
}
// fetch() transparently decodes gzip unless asked not to, so the gzip check reads the wire through node:http.
function postRaw(body, headers) {
  return new Promise((ok, no) => {
    const u = new URL(URL_);
    const rq = http.request({ host: u.hostname, port: u.port, path: u.pathname, method: "POST", headers }, (rs) => {
      const chunks = []; rs.on("data", (c) => chunks.push(c)); rs.on("end", () => ok({ status: rs.statusCode, headers: rs.headers, body: Buffer.concat(chunks) }));
    });
    rq.on("error", no); rq.end(body);
  });
}
function socket(frame, endsOn) {
  return new Promise((ok, no) => {
    const ws = new WebSocket(WS); const got = [];
    const t = setTimeout(() => { ws.close(); no(new Error("ws timeout")); }, 30000);
    ws.onopen = () => ws.send(frame);
    ws.onerror = (e) => { clearTimeout(t); no(e); };
    ws.onmessage = (e) => { if (String(e.data).startsWith('["AUTH"')) return; got.push(String(e.data)); if (endsOn(JSON.parse(e.data))) { clearTimeout(t); ws.close(); ok(got); } };
  });
}
const REQ = (sub, ...filters) => JSON.stringify(["REQ", sub, ...filters]);
const COUNT = (sub, ...filters) => JSON.stringify(["COUNT", sub, ...filters]);
const EVENT = (e) => JSON.stringify(["EVENT", e]);
const onSocket = (frame) => socket(frame, (m) => ["EOSE", "CLOSED", "COUNT", "OK", "NOTICE"].includes(m[0]));

// ---------------------------------------------------------------- checks
let pass = 0, fail = 0;
const check = (name, cond, detail = "") => { if (cond) { pass++; console.log("  ok   " + name); } else { fail++; console.log("  FAIL " + name + (detail ? "  -- " + detail : "")); } };
const ids = (lines) => lines.map((l) => JSON.parse(l)).filter((m) => m[0] === "EVENT").map((m) => m[2].id);

console.log("signature self-check");
{
  const k = keypair(); const e = event(k, 1, [], "self-check");
  const r = await post(EVENT(e));
  check("the harness's own Schnorr signature is accepted by the relay", r.status === 200 && JSON.parse(r.lines[0])[2] === true, r.lines[0]);
}

console.log("reads through the lens gate");
{
  const r = await post(REQ("gate", { kinds: [1], limit: 5 }));
  check("an anonymous REQ with no lens is 401", r.status === 401, `${r.status} ${r.lines[0]}`);
  check("... with WWW-Authenticate: Nostr", r.headers.get("www-authenticate") === "Nostr");
  check("... and one CLOSED line under the client's id, auth-required:", r.lines.length === 1 && r.lines[0].startsWith('["CLOSED","gate","auth-required:'), r.lines[0]);
}

console.log("REQ: the same frames as the socket, production filters");
const filters = [
  { kinds: [0], limit: 200, search: "include:spam" },
  { kinds: [1], limit: 300, search: "include:spam" },
  { kinds: [30392, 30393, 30394, 30395], limit: 500, search: "include:spam" },
  { kinds: [10040], limit: 500, search: "include:spam" },
  { kinds: [1985], limit: 300, search: "include:spam" },
  { kinds: [0], limit: 50, search: `bitcoin observer:${RANKER}` },
  { kinds: [30392], limit: 50, search: `music observer:${RANKER}` },
];
for (const [i, f] of filters.entries()) {
  const frame = REQ(`p${i}`, f);
  const h = await post(frame); const w = await onSocket(frame);
  const label = JSON.stringify(f).slice(0, 70);
  check(`${label}: 200, ${ids(h.lines).length} events, ends on ["EOSE","p${i}"]`, h.status === 200 && h.lines.at(-1) === `["EOSE","p${i}"]`, `${h.status} ${h.lines.at(-1)}`);
  check(`${label}: line for line what the socket sent (${w.length} frames)`, JSON.stringify(h.lines) === JSON.stringify(w), `http ${h.lines.length} ws ${w.length}`);
  check(`${label}: application/x-ndjson, no-store`, (h.headers.get("content-type") || "").startsWith("application/x-ndjson") && h.headers.get("cache-control") === "no-store");
}
{
  const frame = REQ("or", { kinds: [0], limit: 20, search: "include:spam" }, { kinds: [1985], limit: 20, search: "include:spam" });
  const h = await post(frame); const w = await onSocket(frame);
  check("two filters in one REQ are ORed, as on the socket", h.status === 200 && JSON.stringify(h.lines) === JSON.stringify(w));
}

console.log("COUNT");
for (const [i, f] of [{ kinds: [0], search: "include:spam" }, { kinds: [1], search: "include:spam" }, { kinds: [30392, 30393, 30394, 30395], search: "include:spam" }].entries()) {
  const frame = COUNT(`c${i}`, f);
  const h = await post(frame); const w = await onSocket(frame);
  check(`COUNT ${JSON.stringify(f.kinds)}: one line, the socket's (${h.lines[0]})`, h.status === 200 && h.lines.length === 1 && h.lines[0] === w.at(-1), `${h.lines[0]} vs ${w.at(-1)}`);
}

console.log("gzip");
{
  const body = REQ("z", { kinds: [0], limit: 300, search: "include:spam" });
  const z = await postRaw(body, { "accept-encoding": "gzip", "content-length": Buffer.byteLength(body) });
  const plain = await post(body);
  check("Content-Encoding: gzip on the wire", z.headers["content-encoding"] === "gzip", JSON.stringify(z.headers));
  const lines = gunzipSync(z.body).toString().split("\n").filter(Boolean);
  check(`decodes to the same ${lines.length} lines as the plain answer`, JSON.stringify(lines) === JSON.stringify(plain.lines));
  check(`and is smaller (${z.body.length} vs ${Buffer.byteLength(plain.lines.join("\n"))} bytes)`, z.body.length < Buffer.byteLength(plain.lines.join("\n")));
  check("X-Accel-Buffering: no", z.headers["x-accel-buffering"] === "no");
}

console.log("NIP-98 sign-in");
{
  const key = keypair(); const body = REQ("n", { kinds: [0], limit: 20 });
  const tok = nip98(key, URL_, body);
  const a = await post(body, { authorization: tok });
  check("a signed REQ with no lens token is served (the signer is the lens)", a.status === 200 && a.lines.at(-1) === '["EOSE","n"]', `${a.status} ${a.lines[0]}`);
  const b = await post(body, { authorization: tok });
  check("the same token again is served too: not single-use", b.status === 200, `${b.status} ${b.lines[0]}`);
  const c = await post(REQ("n", { kinds: [1], limit: 20 }), { authorization: tok });
  check("that token on another body is 401 payload, the REQ's own CLOSED", c.status === 401 && c.lines[0].startsWith('["CLOSED","n",') && c.lines[0].includes("payload"), c.lines[0]);
  const d = await post(body, { authorization: nip98(key, "https://elsewhere.example/", body) });
  check("a token for another url is 401 url mismatch", d.status === 401 && d.lines[0].includes("url"), d.lines[0]);
  const e = await post(body, { authorization: nip98(key, URL_.replace(/\/$/, ""), body) });
  check("the relay's url without its trailing slash is the same url", e.status === 200, `${e.status} ${e.lines[0]}`);
  const f = await post(body, { authorization: "Basic dXNlcjpwYXNz" });
  check("another Authorization scheme is ignored: anonymous, 401 no lens", f.status === 401 && f.lines[0].includes("auth-required"), f.lines[0]);
}

console.log("EVENT");
{
  const key = keypair();
  const note = event(key, 1, [["t", "nipfe"]], "NIP-FE end to end " + Date.now());
  const a = await post(EVENT(note));
  check("a signed note is 200 with one OK true line", a.status === 200 && a.lines.length === 1 && JSON.stringify(JSON.parse(a.lines[0]).slice(0, 3)) === JSON.stringify(["OK", note.id, true]), a.lines[0]);
  const back = await post(REQ("back", { ids: [note.id], search: "include:spam" }));
  check("and is then served over HTTP", ids(back.lines).includes(note.id), back.lines.join(" "));
  const wsBack = await onSocket(REQ("back", { ids: [note.id], search: "include:spam" }));
  check("and by the websocket", wsBack.some((l) => l.includes(note.id)));
  const again = await post(EVENT(note));
  check("the same event again is 200 (duplicate)", again.status === 200 && again.lines[0].includes("duplicate"), again.lines[0]);
  const bad = await post(EVENT({ ...note, content: "tampered" }));
  check("a forged event is 400 with OK false", bad.status === 400 && JSON.parse(bad.lines[0])[2] === false, bad.lines[0]);
  const newer = event(key, 0, [], '{"name":"newer"}', Math.floor(Date.now() / 1000));
  const older = event(key, 0, [], '{"name":"older"}', Math.floor(Date.now() / 1000) - 3600);
  await post(EVENT(newer));
  const stale = await post(EVENT(older));
  check("a stale replaceable is OK false replaced: -> 400", stale.status === 400 && stale.lines[0].includes("replaced:"), `${stale.status} ${stale.lines[0]}`);
}

console.log("refusals before a command runs are one NOTICE line");
for (const [body, status] of [
  ["", 400],
  ["not json", 400],
  ["[]", 400],
  [JSON.stringify({ kinds: [1] }), 400],
  ['["REQ","q",1]', 400],
  ['["CLOSE","q"]', 400],
  ['["AUTH",{}]', 400],
  [REQ("big", { search: "include:spam " + "x".repeat(300000) }), 413],
]) {
  const r = await post(body);
  check(`${body.slice(0, 24) || "(empty)"}${body.length > 24 ? "…" : ""} -> ${status} NOTICE`, r.status === status && r.lines.length === 1 && r.lines[0].startsWith('["NOTICE",'), `${r.status} ${r.lines[0]?.slice(0, 120)}`);
}
{
  const deep = '["REQ","d",' + '{"a":'.repeat(3000) + "1" + "}".repeat(3000) + "]";
  const r = await post(deep);
  check("a 3000-deep body is a 400, and the relay is still up", r.status === 400 && (await post(COUNT("up", { kinds: [0], search: "include:spam" }))).status === 200, `${r.status} ${r.lines[0]?.slice(0, 100)}`);
}

console.log("NIP-86 shares the URL");
{
  const r = await post(JSON.stringify({ method: "supportedmethods", params: [] }), { "content-type": "application/nostr+json+rpc" });
  check("an application/nostr+json+rpc POST is not answered as a command", !(r.headers.get("content-type") || "").startsWith("application/x-ndjson"), `${r.status} ${r.headers.get("content-type")}`);
  const t = await post(REQ("ct", { kinds: [0], limit: 1, search: "include:spam" }), { "content-type": "text/plain" });
  check("any other Content-Type is a command", t.status === 200 && t.lines.at(-1) === '["EOSE","ct"]', `${t.status} ${t.lines.at(-1)}`);
}

console.log("full-text search: the same ranked frames as the socket (production words)");
for (const q of ["bitcoin", "nostr", "music", "art", "zap"]) for (const kinds of [[0], [1], [30392, 30393, 30394, 30395]]) {
  const frame = REQ("s", { kinds, limit: 100, search: `${q} include:spam` });
  const h = await post(frame); const w = await onSocket(frame);
  check(`"${q}" kinds ${JSON.stringify(kinds)}: ${ids(h.lines).length} hits, line for line the socket's`, h.status === 200 && JSON.stringify(h.lines) === JSON.stringify(w), `http ${h.lines.length} ws ${w.length} ${h.lines.at(-1)}`);
}

console.log("the per-client gate under 40 concurrent requests");
{
  const body = REQ("g", { kinds: [0, 1, 1985, 10040, 30392, 30393, 30394, 30395], limit: 5000, search: "include:spam" });
  const all = await Promise.all(Array.from({ length: 40 }, () => post(body)));
  const by = {}; for (const r of all) by[r.status] = (by[r.status] || 0) + 1;
  console.log("  statuses:", JSON.stringify(by));
  check("every answer is a 200 or a 429 from the gate", all.every((r) => r.status === 200 || r.status === 429));
  check("the 429s are one NOTICE rate-limited: line and the 200s end on EOSE", all.every((r) => (r.status === 429 ? r.lines[0].startsWith('["NOTICE","rate-limited:') : r.lines.at(-1) === '["EOSE","g"]')));
  check("some were refused at the default HTTP_RELAY_PER_CLIENT", (by[429] || 0) > 0);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
