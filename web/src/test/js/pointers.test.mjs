// The follow-up read behind the provenance row: which filters a page sends, and what the row
// makes of the answer. Every declaration filter carries `authors`, and a kind the Map delegates
// to nobody is not asked for.
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

// The Map's two real shapes: a NIP-85 dimension and a bare kind.
const MAP = delegationsOf({ tags: [["30382:rank", SCORER, ""], ["30392", LISTER, ""]] });
const READER = hex("7");
const TRUST = trustedSigners(MAP, READER);

const byKind = (filters) => {
  const m = new Map();
  for (const f of filters) m.set(`${f.kinds[0]}${Object.keys(f).find((k) => k.startsWith("#"))}`, f);
  return m;
};

{
  const page = [
    profile("1", ALICE),
    ev("2", BOB, 1),                                     // a note: an id, no profile
    ev("3", BOB, 30023, [["d", "post"]]),                // addressable: an id and an address
  ];
  const t = targetsOf(page);
  assert.deepStrictEqual(t.pubkeys, [ALICE],
    "pubkeys are the authors of the kind-0s HERE — a pill about somebody is drawn on their profile card, and a page without it has no target");
  assert.deepStrictEqual(t.ids, [hex("1"), hex("2"), hex("3")]);
  assert.deepStrictEqual(t.addrs, [`30023:${BOB}:post`], "only an addressable event has an address");
  const empty = targetsOf([]);
  assert.deepStrictEqual(pointerFilters(empty, TRUST, { observer: READER }), [], "a page with nothing on it asks nothing");
}

{
  const page = [profile("1", ALICE), ev("2", BOB, 1), ev("3", BOB, 30023, [["d", "post"]])];
  const filters = pointerFilters(targetsOf(page), TRUST, { observer: READER });
  const f = byKind(filters);

  assert.deepStrictEqual(f.get("30392#p").authors, [LISTER, READER],
    "a Trusted List is asked for by its DELEGATED SIGNER — plus the reader, whose own lists the relay unpacks too");
  assert.deepStrictEqual(f.get("30392#p")["#p"], [ALICE], "…and by the members this page could draw");
  assert.deepStrictEqual(f.get("30382#d").authors, [SCORER, READER], "an assertion likewise — its subject is its `d`");
  assert.deepStrictEqual(f.get("30382#d")["#d"], [ALICE]);

  for (const kind of [30393, 30394, 30383, 30384, 30000, 39089]) {
    const f2 = filters.find((x) => x.kinds[0] === kind);
    if (f2) assert.deepStrictEqual(f2.authors, [READER], `kind ${kind}: nobody is delegated, so only the reader speaks`);
  }
  const anon = pointerFilters(targetsOf(page), trustedSigners(new Map(), null), {});
  for (const x of anon) {
    assert.strictEqual(x.kinds[0], 1985,
      "an anonymous reader delegates nobody, so no declaration is asked for at all — an ungated ask would answer with every publisher on the relay");
  }
  assert.strictEqual(filters.some((x) => x.kinds[0] === 30395), false,
    "30395's members are NIP-73 identifiers, which are not events this page can draw");

  for (const x of filters) {
    if (x.kinds[0] === 1985) continue;
    assert.ok(Array.isArray(x.authors) && x.authors.length, `every declaration filter carries authors: ${JSON.stringify(x)}`);
  }
}

{
  const page = [profile("1", ALICE)];
  const filters = pointerFilters(targetsOf(page), trustedSigners(new Map(), null));
  assert.deepStrictEqual([...new Set(filters.map((f) => f.kinds[0]))], [1985],
    "no delegation is not a reason to ask openly");
}

// A page can hold somebody the reader put on a NIP-51 list; the reason is asked for like any
// other declaration.
{
  const page = [profile("1", ALICE), ev("2", BOB, 1)];
  const f = byKind(pointerFilters(targetsOf(page), TRUST, { observer: READER }));

  assert.deepStrictEqual(f.get("30000#p").authors, [READER],
    "a people list is asked for on the reader's OWN behalf — a Map that names no publisher for it leaves only them");
  assert.deepStrictEqual(f.get("30000#p")["#p"], [ALICE], "…by member, the same tag a 30392 is asked by");
  assert.deepStrictEqual(f.get("39089#p").authors, [READER], "and a follow pack the same way");
  assert.strictEqual("search" in f.get("30000#p"), false,
    "no lens on a declaration filter: it is already narrowed to keys this reader named");

  const withList = trustedSigners(delegationsOf({ tags: [["30000", LISTER, ""]] }), READER);
  assert.deepStrictEqual(byKind(pointerFilters(targetsOf(page), withList, {})).get("30000#p").authors, [LISTER, READER],
    "a Map that names a curator for 30000 asks that curator too");

  const anon = pointerFilters(targetsOf(page), trustedSigners(new Map(), null), {});
  assert.strictEqual(anon.some((x) => x.kinds[0] === 30000 || x.kinds[0] === 39089), false,
    "nobody signed in means nobody's curation to ask about");
}

{
  const page = [profile("1", ALICE), ev("2", BOB, 1)];
  const labels = pointerFilters(targetsOf(page), TRUST, { observer: READER }).filter((f) => f.kinds[0] === 1985);
  assert.strictEqual(labels.length, 2, "asked by every tag that can name something on screen and has one");
  for (const f of labels) {
    assert.strictEqual("authors" in f, false, "NIP-32 is open by construction — there is no author list to narrow one");
    assert.strictEqual(f.limit, LABEL_LIMIT, "…so a limit is one of the two bounds there are");
    // With no `authors`, the reader's trust floor is the other bound.
    assert.strictEqual(f.search, `observer:${READER}`, "a label read is made through the observer's eyes");
  }
  // A lens on a declaration filter would drop the reader's own provider's lists.
  for (const f of pointerFilters(targetsOf(page), TRUST, { observer: READER })) {
    if (f.kinds[0] === 1985) continue;
    assert.strictEqual("search" in f, false, "a declaration filter declares no lens — the socket waives it");
  }
  const anonLabels = pointerFilters(targetsOf(page), trustedSigners(new Map(), null), {});
  for (const f of anonLabels) assert.strictEqual("search" in f, false, "nobody's eyes: the socket's own include:spam is the honest declaration");
  assert.strictEqual(pointerFilters(targetsOf(page), TRUST, { labels: false }).some((f) => f.kinds[0] === 1985), false);

  // Two asks, so the small gated read does not wait on the open one's EOSE.
  const gated = pointerFilters(targetsOf(page), TRUST, { labels: false, observer: READER });
  const open = pointerFilters(targetsOf(page), TRUST, { declarations: false, observer: READER });
  assert.ok(gated.length && open.length, "each half is an ask of its own");
  assert.ok(gated.every((f) => f.kinds[0] !== 1985), "the gated half carries no label filter");
  assert.ok(open.every((f) => f.kinds[0] === 1985), "and the open half carries nothing else");
  assert.strictEqual(gated.length + open.length,
    pointerFilters(targetsOf(page), TRUST, { observer: READER }).length,
    "…and together they are exactly what one ask would have been");
  assert.deepStrictEqual(pointerFilters(targetsOf(page), TRUST, { labels: false, declarations: false }), [],
    "neither half is no ask at all");
}

{
  const many = [];
  for (let i = 0; i < BATCH + 5; i++) many.push(profile(`${i}`.padStart(2, "0") + "a", hex(`${i}`.padStart(2, "0") + "f")));
  const lists = pointerFilters(targetsOf(many), TRUST, { observer: READER }).filter((f) => f.kinds[0] === 30392);
  assert.strictEqual(lists.length, 2, "over a batch is two filters, and NIP-01 ORs them inside one REQ");
  assert.strictEqual(lists[0]["#p"].length, BATCH);
  assert.strictEqual(lists[1]["#p"].length, 5);
}

// A pointer the relay spliced is still in the page, so the fold sees some twice.
{
  const page = [profile("1", ALICE), profile("2", BOB)];
  const TRUSTED = TRUST;
  assert.strictEqual(provenanceOf(page, TRUSTED).size, 0, "profiles alone carry no reason for being here");

  const list = ev("9", LISTER, 30392, [["d", "vh"], ["title", "Verified Human"], ["p", ALICE], ["p", BOB]]);
  const label = ev("8", BOT, 1985, [["L", "ugc"], ["l", "spammer", "ugc"], ["p", BOB]]);

  const rows = provenanceOf([...page, list, label], TRUSTED);
  assert.deepStrictEqual(rows.get(hex("1")).map((p) => p.text), ["Verified Human"]);
  assert.deepStrictEqual(rows.get(hex("2")).map((p) => p.text), ["Verified Human", "spammer"],
    "delegated first, then the open one — the tone is the whole distinction");
  assert.strictEqual(rows.get(hex("1"))[0].gated, true);
  assert.strictEqual(rows.get(hex("2"))[1].gated, false);

  const both = provenanceOf([...page, list, list, label], TRUSTED);
  assert.strictEqual(both.get(hex("1")).length, 1);
  assert.strictEqual(both.get(hex("1"))[0].count, 1,
    "an event that arrives both ways is one record — the count is what claims corroboration");
}

console.log("pointers: the gate moves to the client as `authors`, labels stay open and bounded, and the folded answer draws the row");
process.exit(0);

// The render gate is built from the same Map: a key delegated for lists does not score them.
{
  const t = trustedSigners(MAP, READER);
  assert.deepStrictEqual([...t.get(30392)], [LISTER, READER], "the service key for the kind, plus the reader themselves");
  assert.deepStrictEqual([...t.get(30382)], [SCORER, READER], "per kind — the list publisher does not speak for assertions");
  assert.deepStrictEqual([...t.get(30393)], [READER],
    "a kind the Map never names trusts only the reader — the relay unpacks a list you signed yourself");
  assert.deepStrictEqual([...trustedSigners(new Map(), null).get(30392)], [],
    "and an anonymous reader, who is nobody, trusts nobody");
  // Dimensions of one kind collapse, the grouping the store's own gate does.
  const twoDims = delegationsOf({ tags: [["30382:rank", SCORER, ""], ["30382:followers", SCORER, ""]] });
  assert.deepStrictEqual([...trustedSigners(twoDims, null).get(30382)], [SCORER]);
}
