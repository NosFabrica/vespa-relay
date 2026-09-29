// End-to-end check of the Neo4j graph projection through a running relay (docs/configuration.md,
// "Graph projection"): writes signed events over the socket, then asks the graph through
// POST /graph/cypher. Needs GRAPH_PROJECTION=on and GRAPH_CYPHER=auth (or admin with this
// script's key) on the relay.
//
//   node relay/tools/graph-e2e.mjs [http://localhost:7777]
//
// It WRITES: throwaway keys publish a follow list and its replacement, a note, a reply, a
// reaction, and a note that is then deleted.
import { createHash, randomBytes } from "node:crypto";

const BASE = (process.argv[2] || "http://localhost:7777").replace(/\/$/, "");
const WS = BASE.replace(/^http/, "ws");

// ---------------------------------------------------------------- secp256k1 / BIP-340 (as nipfe-e2e.mjs)
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
  const x = mod(l * l - p1[0] - p2[0]); return [x, mod(l * (p1[0] - x) - p1[1])];
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
const now = Math.floor(Date.now() / 1000);
const event = (key, kind, tags, content, created_at = now) => {
  const id = hex(sha(JSON.stringify([0, key.pub, created_at, kind, tags, content])));
  return { id, pubkey: key.pub, created_at, kind, tags, content, sig: sign(Buffer.from(id, "hex"), key.d) };
};
const nip98 = (key, url, body) =>
  "Nostr " + Buffer.from(JSON.stringify(event(key, 27235, [["u", url], ["method", "POST"], ["payload", hex(sha(body))]], "", Math.floor(Date.now() / 1000)))).toString("base64");

// ---------------------------------------------------------------- transports
function publish(e) {
  return new Promise((ok, no) => {
    const ws = new WebSocket(WS);
    const t = setTimeout(() => { ws.close(); no(new Error("ws timeout")); }, 30000);
    ws.onopen = () => ws.send(JSON.stringify(["EVENT", e]));
    ws.onerror = (err) => { clearTimeout(t); no(err); };
    ws.onmessage = (m) => { const f = JSON.parse(m.data); if (f[0] === "OK") { clearTimeout(t); ws.close(); ok(f); } };
  });
}
async function cypher(key, query, params = {}, hydrate = true) {
  const body = JSON.stringify({ query, params, hydrate });
  const headers = { "content-type": "application/json" };
  if (key) headers.authorization = nip98(key, BASE + "/graph/cypher", body);
  const r = await fetch(BASE + "/graph/cypher", { method: "POST", body, headers });
  const text = await r.text();
  let json = null; try { json = JSON.parse(text); } catch {}
  return { status: r.status, json, text };
}

let pass = 0, fail = 0;
const check = (name, cond, detail = "") => { if (cond) { pass++; console.log("  ok   " + name); } else { fail++; console.log("  FAIL " + name + (detail ? "  -- " + detail : "")); } };
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------------------------------------------------------------- the story
const alice = keypair(), bob = keypair(), carol = keypair(), asker = keypair();
const followsV1 = event(alice, 3, [["p", bob.pub], ["p", carol.pub]], "", now - 20);
const followsV2 = event(alice, 3, [["p", bob.pub]], "", now - 10);
const note = event(bob, 1, [["t", "graphe2e"]], "hello graph");
const reply = event(carol, 1, [["e", note.id, "", "root"], ["p", bob.pub]], "a reply");
const reaction = event(alice, 7, [["e", note.id], ["p", bob.pub]], "+");
const doomed = event(carol, 1, [], "to be deleted", now - 5);
const deletion = event(carol, 5, [["e", doomed.id]], "");

for (const e of [followsV1, note, reply, reaction, doomed, followsV2, deletion]) {
  const ok = await publish(e);
  check(`relay accepted kind ${e.kind}`, ok[2] === true, JSON.stringify(ok));
}
await sleep(3000); // the feed is asynchronous by design

let r = await cypher(asker, "MATCH (u:User {pubkey: $pk}) RETURN COUNT { (u)<-[:p_3]-() } AS followers", { pk: bob.pub });
check("bob has one follower", r.status === 200 && r.json?.rows?.[0]?.[0] === 1, r.text);
r = await cypher(asker, "MATCH (u:User {pubkey: $pk}) RETURN COUNT { (u)<-[:p_3]-() } AS followers", { pk: carol.pub });
check("the replaced follow list no longer follows carol", r.status === 200 && r.json?.rows?.[0]?.[0] === 0, r.text);
r = await cypher(asker, "MATCH (:Event {id: $id})<-[e:e_1]-(n:Stored) WHERE 'reply' IN e.roles RETURN n", { id: note.id });
check("the reply is found and hydrated from Vespa", r.status === 200 && r.json?.rows?.[0]?.[0]?.content === "a reply", r.text);
r = await cypher(asker, "MATCH (:Event {id: $id})<-[x:e_7]-(re:Stored) WHERE 'reaction' IN x.roles RETURN re.content", { id: note.id }, false);
check("the reaction symbol is on the graph", r.status === 200 && r.json?.rows?.[0]?.[0] === "+", r.text);
r = await cypher(asker, "OPTIONAL MATCH (n:Event:Stored {id: $id}) RETURN n IS NOT NULL AS held", { id: doomed.id });
check("the deleted note is gone from the graph", r.status === 200 && r.json?.rows?.[0]?.[0] === false, r.text);
r = await cypher(asker, "MATCH (n:Event:Stored {kind: 5, id: $id}) RETURN n.id", { id: deletion.id }, false);
check("the deletion itself is projected", r.status === 200 && r.json?.rows?.length === 1, r.text);
r = await cypher(asker, "MATCH (:Tag {key: 't:graphe2e'})<-[:t_1]-(n:Stored {id: $id}) RETURN count(n)", { id: note.id });
check("the hashtag node joins the note", r.status === 200 && r.json?.rows?.[0]?.[0] === 1, r.text);

r = await cypher(asker, "MATCH (n) DETACH DELETE n");
check("a write is refused with 400", r.status === 400, r.text);
r = await cypher(asker, "LOAD CSV FROM 'file:///etc/passwd' AS l RETURN l");
check("LOAD CSV is refused with 400", r.status === 400, r.text);
r = await cypher(null, "RETURN 1");
check("an unsigned call is 401", r.status === 401, r.text);
{
  const body = JSON.stringify({ query: "RETURN 1" });
  const auth = nip98(asker, BASE + "/graph/cypher", JSON.stringify({ query: "RETURN 2" }));
  const rr = await fetch(BASE + "/graph/cypher", { method: "POST", body, headers: { authorization: auth } });
  check("a token signed for another body is 401", rr.status === 401, String(rr.status));
}
const schema = await (await fetch(BASE + "/graph/schema")).json();
check("GET /graph/schema lists p_3", (schema.relationship_types?.p_3 ?? 0) >= 1, JSON.stringify(schema).slice(0, 200));
const health = await (await fetch(BASE + "/graph/health.json")).json();
check("GET /graph/health.json reports no drops", health.queue?.dropped === 0, JSON.stringify(health));

console.log(`\n${pass} passed, ${fail} failed`);
process.exit(fail === 0 ? 0 : 1);
