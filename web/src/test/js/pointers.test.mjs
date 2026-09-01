// The follow-up read behind the provenance row: which filters a page sends
// once the relay stops splicing pointers into the answer, and what the row
// makes of what comes back.
//
// The property under test is the GATE. It used to live on the serving side —
// a declaration reached a search result only if the reader's Map delegated its
// signer, so "it arrived" was the verdict — and asking the anonymous reference
// socket re-opens it: that socket narrows nothing, and a bare `kinds:[30392]`
// by member answers with every publisher's list. Measured against staging on
// 2026-09-01, seven lists name two probed members and only six are from the
// publisher this reader delegated. So every declaration filter must carry
// `authors`, and a kind the Map delegates to nobody must not be asked for at
// all.
import assert from 'assert';

globalThis.location = { protocol: "http:", host: "localhost:7787" };
globalThis.window = { addEventListener: () => {} };
globalThis.WebSocket = class { constructor() { this.readyState = 0; } send() {} close() {} };

const { targetsOf, pointerFilters, BATCH, LABEL_LIMIT, trustedSigners } =
  await import(new URL("../../main/resources/web/shared/pointers.js", import.meta.url));
const { delegationsOf } = await import(new URL("../../main/resources/web/shared/providers.js", import.meta.url));
const { provenanceOf } = await import(new URL("../../main/resources/web/provenance.js", import.meta.url));

const hex = (c) => String(c).repeat(64).slice(0, 64);
const LISTER = hex("b"), SCORER = hex("c"), BOT = hex("d");
const ALICE = hex("1"), BOB = hex("2");
const ev = (id, pubkey, kind, tags = []) => ({ id: hex(id), pubkey, kind, created_at: 1, tags, content: "" });
const profile = (id, pubkey) => ({ ...ev(id, pubkey, 0), content: "{}" });

// The Map's two real shapes — a NIP-85 dimension and a bare kind.
const MAP = delegationsOf({ tags: [["30382:rank", SCORER, ""], ["30392", LISTER, ""]] });

const byKind = (filters) => {
  const m = new Map();
  for (const f of filters) m.set(`${f.kinds[0]}${Object.keys(f).find((k) => k.startsWith("#"))}`, f);
  return m;
};

// ---- what a page can have a pill drawn ON --------------------------------
{
  const page = [
    profile("1", ALICE),
    ev("2", BOB, 1),                                     // a note: an id, no profile
    ev("3", BOB, 30023, [["d", "post"]]),                // addressable: an id AND an address
  ];
  const t = targetsOf(page);
  assert.deepStrictEqual(t.pubkeys, [ALICE],
    "pubkeys are the authors of the kind-0s HERE — a pill about somebody is drawn on their profile card, and a page without it has no target");
  assert.deepStrictEqual(t.ids, [hex("1"), hex("2"), hex("3")]);
  assert.deepStrictEqual(t.addrs, [`30023:${BOB}:post`], "only an addressable event has an address");
  const empty = targetsOf([]);
  assert.deepStrictEqual(pointerFilters(empty, MAP), [], "a page with nothing on it asks nothing");
}

// ---- THE GATE ------------------------------------------------------------
{
  const page = [profile("1", ALICE), ev("2", BOB, 1), ev("3", BOB, 30023, [["d", "post"]])];
  const filters = pointerFilters(targetsOf(page), MAP);
  const f = byKind(filters);

  assert.deepStrictEqual(f.get("30392#p").authors, [LISTER], "a Trusted List is asked for BY ITS DELEGATED SIGNER");
  assert.deepStrictEqual(f.get("30392#p")["#p"], [ALICE], "…and by the members this page could draw");
  assert.deepStrictEqual(f.get("30382#d").authors, [SCORER], "an assertion likewise — its subject is its `d`");
  assert.deepStrictEqual(f.get("30382#d")["#d"], [ALICE]);

  for (const kind of [30393, 30394, 30383, 30384]) {
    assert.strictEqual(filters.some((x) => x.kinds[0] === kind), false,
      `kind ${kind} is delegated to nobody, so it is not asked for — an ungated ask would answer with every publisher on the relay`);
  }
  assert.strictEqual(filters.some((x) => x.kinds[0] === 30395), false,
    "30395's members are NIP-73 identifiers, which are not events this page can draw");

  for (const x of filters) {
    if (x.kinds[0] === 1985) continue;
    assert.ok(Array.isArray(x.authors) && x.authors.length, `every declaration filter carries authors: ${JSON.stringify(x)}`);
  }
}

// A reader with no Map at all asks for no declaration — only the open half.
{
  const page = [profile("1", ALICE)];
  const filters = pointerFilters(targetsOf(page), new Map());
  assert.deepStrictEqual([...new Set(filters.map((f) => f.kinds[0]))], [1985],
    "no delegation is not a reason to ask openly");
}

// ---- labels are ungated, and bounded by the only thing left --------------
{
  const page = [profile("1", ALICE), ev("2", BOB, 1)];
  const labels = pointerFilters(targetsOf(page), MAP).filter((f) => f.kinds[0] === 1985);
  assert.strictEqual(labels.length, 2, "asked by every tag that can name something on screen and has one");
  for (const f of labels) {
    assert.strictEqual("authors" in f, false, "NIP-32 is open by construction — there is no author list to narrow one");
    assert.strictEqual(f.limit, LABEL_LIMIT, "…so a limit is the only bound there is");
  }
  assert.strictEqual(pointerFilters(targetsOf(page), MAP, { labels: false }).some((f) => f.kinds[0] === 1985), false);
}

// ---- batching ------------------------------------------------------------
{
  const many = [];
  for (let i = 0; i < BATCH + 5; i++) many.push(profile(`${i}`.padStart(2, "0") + "a", hex(`${i}`.padStart(2, "0") + "f")));
  const lists = pointerFilters(targetsOf(many), MAP).filter((f) => f.kinds[0] === 30392);
  assert.strictEqual(lists.length, 2, "over a batch is two filters, and NIP-01 ORs them inside one REQ");
  assert.strictEqual(lists[0]["#p"].length, BATCH);
  assert.strictEqual(lists[1]["#p"].length, 5);
}

// ---- and the row the answer produces --------------------------------------
//
// The whole point: a page of profiles alone draws nothing, and the same page
// with the fetched pointers folded in draws exactly what the spliced answer
// used to. Seeded from BOTH — a pointer the relay did send is still in the
// page — and one that arrives twice must still be one pill with a count of 1.
{
  const page = [profile("1", ALICE), profile("2", BOB)];
  const TRUSTED = trustedSigners(MAP);
  assert.strictEqual(provenanceOf(page, TRUSTED).size, 0, "profiles alone carry no reason for being here");

  const list = ev("9", LISTER, 30392, [["d", "vh"], ["title", "Verified Human"], ["p", ALICE], ["p", BOB]]);
  const label = ev("8", BOT, 1985, [["L", "ugc"], ["l", "spammer", "ugc"], ["p", BOB]]);

  const rows = provenanceOf([...page, list, label], TRUSTED);
  assert.deepStrictEqual(rows.get(hex("1")).map((p) => p.text), ["Verified Human"]);
  assert.deepStrictEqual(rows.get(hex("2")).map((p) => p.text), ["Verified Human", "spammer"],
    "delegated first, then the open one — the tone is the whole distinction");
  assert.strictEqual(rows.get(hex("1"))[0].gated, true);
  assert.strictEqual(rows.get(hex("2"))[1].gated, false);

  // The relay spliced the list AND the fetch returned it: still one pill.
  const both = provenanceOf([...page, list, list, label], TRUSTED);
  assert.strictEqual(both.get(hex("1")).length, 1);
  assert.strictEqual(both.get(hex("1"))[0].count, 1,
    "an event that arrives both ways is one record — the count is what claims corroboration");
}

console.log("pointers: the gate moves to the client as `authors`, labels stay open and bounded, and the folded answer draws the row");
process.exit(0);

// ---- and the render gate is built from the same Map ------------------------
//
// One rule for the ask and for what is drawn: the key a Map names for 30392
// speaks for 30392, and for nothing else. A publisher the reader delegated for
// LISTS does not thereby get to score them.
{
  const t = trustedSigners(MAP);
  assert.deepStrictEqual([...t.get(30392)], [LISTER]);
  assert.deepStrictEqual([...t.get(30382)], [SCORER], "per kind — the list publisher does not speak for assertions");
  assert.deepStrictEqual([...t.get(30393)], [], "a kind the Map never names trusts nobody");
  // Dimensions of ONE kind do collapse: both are 30382 assertions the reader
  // asked one key for, which is the grouping the store's own gate does.
  const twoDims = delegationsOf({ tags: [["30382:rank", SCORER, ""], ["30382:followers", SCORER, ""]] });
  assert.deepStrictEqual([...trustedSigners(twoDims).get(30382)], [SCORER]);
  assert.strictEqual(trustedSigners(new Map()).get(30392).size, 0, "no Map delegates nobody");
}
