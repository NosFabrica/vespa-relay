// Drives NIP-FE (`POST /req`, `/count`, `/event`) end to end against a RUNNING relay and checks
// each answer against the same command on its websocket.
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
// What it covers: the lens gate's 401; REQ over production filters and full-text searches, same
// events in the same order as the socket; COUNT; gzip on the wire; NIP-98 (signed in without a lens,
// not single-use, bound to its body, url and path, other schemes ignored); EVENT (OK, served after,
// duplicate, forged, stale replaceable); every body refusal; a deep body; and the per-client gate
// under 40 concurrent requests, which expects the default HTTP_RELAY_PER_CLIENT.
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
async function post(path, body, headers = {}) {
  const r = await fetch(BASE + path, { method: "POST", body, headers: { "accept-encoding": "identity", ...headers } });
  const raw = Buffer.from(await r.arrayBuffer());
  const text = (r.headers.get("content-encoding") === "gzip" ? gunzipSync(raw) : raw).toString();
  return { status: r.status, headers: r.headers, lines: text.split("\n").filter(Boolean) };
}
// fetch() transparently decodes gzip unless asked not to, so the gzip check reads the wire through node:http.
function postRaw(path, body, headers) {
  return new Promise((ok, no) => {
    const u = new URL(BASE + path);
    const rq = http.request({ host: u.hostname, port: u.port, path: u.pathname, method: "POST", headers }, (rs) => {
      const chunks = []; rs.on("data", (c) => chunks.push(c)); rs.on("end", () => ok({ status: rs.statusCode, headers: rs.headers, body: Buffer.concat(chunks) }));
    });
    rq.on("error", no); rq.end(body);
  });
}
function socket(frames, endsOn) {
  return new Promise((ok, no) => {
    const ws = new WebSocket(WS); const got = [];
    const t = setTimeout(() => { ws.close(); no(new Error("ws timeout")); }, 30000);
    ws.onopen = () => frames.forEach((f) => ws.send(JSON.stringify(f)));
    ws.onerror = (e) => { clearTimeout(t); no(e); };
    ws.onmessage = (e) => { const m = JSON.parse(e.data); if (m[0] === "AUTH") return; got.push(m); if (endsOn(m)) { clearTimeout(t); ws.close(); ok(got); } };
  });
}
const wsReq = (filters) => socket([["REQ", "s", ...filters]], (m) => m[0] === "EOSE" || m[0] === "CLOSED");
const wsCount = (filters) => socket([["COUNT", "c", ...filters]], (m) => m[0] === "COUNT" || m[0] === "CLOSED");

// ---------------------------------------------------------------- checks
let pass = 0, fail = 0;
const check = (name, cond, detail = "") => { if (cond) { pass++; console.log("  ok   " + name); } else { fail++; console.log("  FAIL " + name + (detail ? "  -- " + detail : "")); } };
const ids = (lines) => lines.map((l) => JSON.parse(l)).filter((m) => m[0] === "EVENT").map((m) => m[1].id);
const sameSet = (a, b) => a.length === b.length && a.every((x) => b.includes(x));

console.log("signature self-check");
{
  const k = keypair(); const e = event(k, 1, [], "self-check");
  const r = await post("/event", JSON.stringify(e));
  check("the harness's own Schnorr signature is accepted by the relay", r.status === 200 && JSON.parse(r.lines[0])[2] === true, r.lines[0]);
}

console.log("reads through the lens gate");
{
  const r = await post("/req", JSON.stringify({ kinds: [1], limit: 5 }));
  check("an anonymous REQ with no lens is 401", r.status === 401, `${r.status} ${r.lines[0]}`);
  check("... with WWW-Authenticate: Nostr", r.headers.get("www-authenticate") === "Nostr");
  check("... and one CLOSED auth-required: line, no sub id", r.lines.length === 1 && r.lines[0].startsWith('["CLOSED","auth-required:'), r.lines[0]);
}

console.log("REQ parity with the websocket, production filters");
const filters = [
  { kinds: [0], limit: 200, search: "include:spam" },
  { kinds: [1], limit: 300, search: "include:spam" },
  { kinds: [30392, 30393, 30394, 30395], limit: 500, search: "include:spam" },
  { kinds: [10040], limit: 500, search: "include:spam" },
  { kinds: [1985], limit: 300, search: "include:spam" },
  { kinds: [0], limit: 50, search: `bitcoin observer:${RANKER}` },
  { kinds: [30392], limit: 50, search: `music observer:${RANKER}` },
];
for (const f of filters) {
  const h = await post("/req", JSON.stringify(f));
  const w = await wsReq([f]);
  const hid = ids(h.lines), wid = w.filter((m) => m[0] === "EVENT").map((m) => m[2].id);
  const label = JSON.stringify(f).slice(0, 70);
  check(`${label}: 200, ends on ["EOSE"], ${hid.length} events`, h.status === 200 && h.lines.at(-1) === '["EOSE"]', `${h.status} ${h.lines.at(-1)}`);
  check(`${label}: same events, same order as the socket (${wid.length})`, JSON.stringify(hid) === JSON.stringify(wid), `http ${hid.length} ws ${wid.length}`);
  check(`${label}: application/x-ndjson, no-store`, (h.headers.get("content-type") || "").startsWith("application/x-ndjson") && h.headers.get("cache-control") === "no-store");
  check(`${label}: no frame carries a subscription id`, h.lines.every((l) => !l.startsWith('["EVENT","') && !l.startsWith('["EOSE","')));
}
{
  const f1 = { kinds: [0], limit: 20, search: "include:spam" }, f2 = { kinds: [1985], limit: 20, search: "include:spam" };
  const h = await post("/req", JSON.stringify([f1, f2])); const w = await wsReq([f1, f2]);
  check("an array body is the REQ's filters, ORed, as on the socket", sameSet(ids(h.lines), w.filter((m) => m[0] === "EVENT").map((m) => m[2].id)));
}

console.log("COUNT parity");
for (const f of [{ kinds: [0], search: "include:spam" }, { kinds: [1], search: "include:spam" }, { kinds: [30392, 30393, 30394, 30395], search: "include:spam" }]) {
  const h = await post("/count", JSON.stringify(f)); const w = await wsCount([f]);
  const hc = JSON.parse(h.lines[0]), wc = w.at(-1);
  check(`COUNT ${JSON.stringify(f.kinds)}: one ["COUNT",{...}] line = socket (${JSON.stringify(hc[1])})`, h.status === 200 && h.lines.length === 1 && hc[0] === "COUNT" && JSON.stringify(hc[1]) === JSON.stringify(wc[2]), `${h.lines[0]} vs ${JSON.stringify(wc)}`);
}

console.log("gzip");
{
  const body = JSON.stringify({ kinds: [0], limit: 300, search: "include:spam" });
  const z = await postRaw("/req", body, { "accept-encoding": "gzip", "content-length": Buffer.byteLength(body) });
  const plain = await post("/req", body);
  check("Content-Encoding: gzip on the wire", z.headers["content-encoding"] === "gzip", JSON.stringify(z.headers));
  const lines = gunzipSync(z.body).toString().split("\n").filter(Boolean);
  check(`decodes to the same ${lines.length} lines as the plain answer`, JSON.stringify(lines) === JSON.stringify(plain.lines));
  check(`and is smaller (${z.body.length} vs ${Buffer.byteLength(plain.lines.join("\n"))} bytes)`, z.body.length < Buffer.byteLength(plain.lines.join("\n")));
  check("X-Accel-Buffering: no", z.headers["x-accel-buffering"] === "no");
}

console.log("NIP-98 sign-in");
{
  const key = keypair(); const body = JSON.stringify({ kinds: [0], limit: 20 });
  const tok = nip98(key, BASE + "/req", body);
  const a = await post("/req", body, { authorization: tok });
  check("a signed REQ with no lens token is served (the signer is the lens)", a.status === 200 && a.lines.at(-1) === '["EOSE"]', `${a.status} ${a.lines[0]}`);
  const b = await post("/req", body, { authorization: tok });
  check("the same token again is served too: not single-use", b.status === 200, `${b.status} ${b.lines[0]}`);
  const c = await post("/req", JSON.stringify({ kinds: [1], limit: 20 }), { authorization: tok });
  check("that token on another body is 401 payload", c.status === 401 && c.lines[0].includes("payload"), c.lines[0]);
  const d = await post("/req", body, { authorization: nip98(key, "https://elsewhere.example/req", body) });
  check("a token for another url is 401 url mismatch", d.status === 401 && d.lines[0].includes("url"), d.lines[0]);
  const e = await post("/req", body, { authorization: nip98(key, BASE + "/count", body) });
  check("a token for another path is 401", e.status === 401, e.lines[0]);
  const f = await post("/req", body, { authorization: "Basic dXNlcjpwYXNz" });
  check("another Authorization scheme is ignored: anonymous, 401 no lens", f.status === 401 && f.lines[0].includes("auth-required"), f.lines[0]);
}

console.log("EVENT");
{
  const key = keypair();
  const note = event(key, 1, [["t", "nipfe"]], "NIP-FE end to end " + Date.now());
  const a = await post("/event", JSON.stringify(note));
  check("a signed note is 200 with one OK true line", a.status === 200 && a.lines.length === 1 && JSON.stringify(JSON.parse(a.lines[0]).slice(0, 3)) === JSON.stringify(["OK", note.id, true]), a.lines[0]);
  const back = await post("/req", JSON.stringify({ ids: [note.id], search: "include:spam" }));
  check("and is then served by /req", ids(back.lines).includes(note.id), back.lines.join(" "));
  const wsBack = await wsReq([{ ids: [note.id], search: "include:spam" }]);
  check("and by the websocket", wsBack.some((m) => m[0] === "EVENT" && m[2].id === note.id));
  const again = await post("/event", JSON.stringify(note));
  check("the same event again is 200 (duplicate)", again.status === 200 && again.lines[0].includes("duplicate"), again.lines[0]);
  const forged = { ...note, content: "tampered" };
  const bad = await post("/event", JSON.stringify(forged));
  check("a forged event is 400 with OK false", bad.status === 400 && JSON.parse(bad.lines[0])[2] === false, bad.lines[0]);
  const newer = event(key, 0, [], '{"name":"newer"}', Math.floor(Date.now() / 1000));
  const older = event(key, 0, [], '{"name":"older"}', Math.floor(Date.now() / 1000) - 3600);
  await post("/event", JSON.stringify(newer));
  const stale = await post("/event", JSON.stringify(older));
  check("a stale replaceable is OK false replaced: -> 400", stale.status === 400 && stale.lines[0].includes("replaced:"), `${stale.status} ${stale.lines[0]}`);
}

console.log("bodies");
for (const [path, body, status, starts] of [
  ["/req", "", 400, '["CLOSED","invalid:'],
  ["/req", "not json", 400, '["CLOSED","invalid:'],
  ["/req", "[]", 400, '["CLOSED","invalid:'],
  ["/event", '[{"id":"x"}]', 400, '["CLOSED","invalid:'],
  ["/req", "[1,2]", 400, '["NOTICE",'],
  ["/req", '["REQ","x",{}]', 400, '["NOTICE",'],
  ["/req", JSON.stringify({ search: "include:spam " + "x".repeat(300000) }), 413, '["CLOSED","invalid:'],
]) {
  const r = await post(path, body);
  check(`${path} ${body.slice(0, 20) || "(empty)"}${body.length > 20 ? "…" : ""} -> ${status} ${starts}`, r.status === status && r.lines[0]?.startsWith(starts), `${r.status} ${r.lines[0]?.slice(0, 120)}`);
}
{
  const deep = '{"a":'.repeat(3000) + "1" + "}".repeat(3000);
  const r = await post("/req", deep);
  check("a 3000-deep body is a 400, and the relay is still up", r.status === 400 && (await post("/count", JSON.stringify({ kinds: [0], search: "include:spam" }))).status === 200, `${r.status} ${r.lines[0]?.slice(0, 100)}`);
}

console.log("full-text search parity (production words)");
for (const q of ["bitcoin", "nostr", "music", "art", "zap"]) for (const kinds of [[0], [1], [30392, 30393, 30394, 30395]]) {
  const f = { kinds, limit: 100, search: `${q} include:spam` };
  const h = await post("/req", JSON.stringify(f)); const w = await wsReq([f]);
  const hid = ids(h.lines), wid = w.filter((m) => m[0] === "EVENT").map((m) => m[2].id);
  check(`"${q}" kinds ${JSON.stringify(kinds)}: ${hid.length} hits, same ranked order as the socket`, h.status === 200 && JSON.stringify(hid) === JSON.stringify(wid), `http ${hid.length} ws ${wid.length} ${h.lines.at(-1)}`);
}

console.log("the per-client gate under 40 concurrent requests");
{
  const body = JSON.stringify({ kinds: [0, 1, 1985, 10040, 30392, 30393, 30394, 30395], limit: 5000, search: "include:spam" });
  const all = await Promise.all(Array.from({ length: 40 }, () => post("/req", body)));
  const by = {}; for (const r of all) by[r.status] = (by[r.status] || 0) + 1;
  console.log("  statuses:", JSON.stringify(by));
  check("every answer is a 200 or a 429 from the gate", all.every((r) => r.status === 200 || r.status === 429));
  check("the 429s say rate-limited: and the 200s end on EOSE", all.every((r) => (r.status === 429 ? r.lines[0].startsWith('["CLOSED","rate-limited:') : r.lines.at(-1) === '["EOSE"]')));
  check("some were refused at the default HTTP_RELAY_PER_CLIENT", (by[429] || 0) > 0);
}

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail ? 1 : 0);
