// The provenance rules: which pills a page produces, collapsed, ordered, and
// attributed. Pure — no DOM, no relay — which is the whole reason the rules
// live in web/provenance.js rather than inside the renderer.
import assert from 'assert';

const P = await import(new URL("../../main/resources/web/provenance.js", import.meta.url));
const { provenanceOf, facesNeeded, QUIET_NAMESPACES } = P;

const hex = (c) => String(c).repeat(64).slice(0, 64);
const READER = hex("a"), LISTER = hex("b"), SCORER = hex("c"), BOT = hex("d"), BOT2 = hex("e");
const ev = (id, pubkey, kind, tags = []) => ({ id: hex(id), pubkey, kind, created_at: 1, tags, content: "" });
const profile = (id, pubkey) => ({ ...ev(id, pubkey, 0), content: "{}" });
// WHOM THIS FIXTURE'S READER DELEGATED. Every declaration kind maps to the two
// publishers these cases sign with, so the signer gate is satisfied by default
// and each case still tests the rule it was written for. The gate has its own
// section at the end.
const DECLARED = [30000, 30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395, 39089];
const trusting = (...signers) => new Map(DECLARED.map((k) => [k, new Set(signers)]));
const TRUSTED = trusting(LISTER, SCORER);
const pillsOn = (page, id) => provenanceOf(page, TRUSTED).get(hex(id)) || [];
const texts = (pills) => pills.map((p) => p.text);

// ---- the collapse, which is the rule the whole design turns on -------------
//
// Two lists titled "Verified Human" is not a bug in the data: the publisher
// computes one per observer, and both name the same person. Rendered one pill
// per EVENT that card reads "Verified Human · Verified Human", which is what
// nearly every rich card on staging looks like.
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", LISTER, 30392, [["d", "y"], ["title", "Verified Human"], ["p", READER]]),
  ];
  const pills = pillsOn(page, "1");
  assert.strictEqual(pills.length, 1, "two lists with one title are one pill");
  assert.strictEqual(pills[0].count, 2, "and the count is where the duplicate goes");
  assert.strictEqual(pills[0].text, "Verified Human");
}
// The same rule against the label side's worst real shape: 68 events, 2
// authors, one word. One pill, and both authors behind it.
{
  const page = [profile("1", READER)];
  for (let i = 0; i < 68; i++) {
    page.push(ev(`${i}`.padStart(2, "0") + "f", i % 2 ? BOT : BOT2, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["p", READER]]));
  }
  const pills = pillsOn(page, "1");
  assert.strictEqual(pills.length, 1, "68 labels saying one word are one pill");
  assert.strictEqual(pills[0].count, 68);
  assert.deepStrictEqual(pills[0].authors.sort(), [BOT, BOT2].sort(), "and it carries both authors");
}

// ONE POINTER NAMING ONE TARGET TWICE IS STILL ONE. Lists in the wild repeat
// entries — clients append without checking, which is why peopleOf dedupes its
// grid — and a naive count reads "Verified Human 2" off a single list, the
// count claiming two where there is one. That is the exact fact the count
// exists to state honestly, so it is the one place a duplicate must not pass.
{
  const one = [profile("1", READER), ev("2", LISTER, 30392, [["d", "x"], ["title", "VH"], ["p", READER], ["p", READER]])];
  assert.strictEqual(pillsOn(one, "1")[0].count, 1, "a list that repeats a member is still one list");

  const note = ev("1", READER, 1);
  const twiceNamed = [note, ev("2", BOT, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["e", hex("1")], ["e", hex("1")]])];
  assert.strictEqual(pillsOn(twiceNamed, "1")[0].count, 1, "a label that names one target twice is still one label");

  const twiceValued = [note, ev("2", BOT, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["l", "zapped", "ugc"], ["e", hex("1")]])];
  assert.strictEqual(pillsOn(twiceValued, "1")[0].count, 1, "and one that repeats a value is still one");

  // Deduped PER POINTER, never globally — two different sources saying the
  // same thing is the count doing its job.
  const twoLists = [profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "VH"], ["p", READER]]),
    ev("3", LISTER, 30392, [["d", "y"], ["title", "VH"], ["p", READER]])];
  assert.strictEqual(pillsOn(twoLists, "1")[0].count, 2, "two lists sharing a title still count 2");
}

// ---- metadata is not provenance -------------------------------------------
//
// ISO-639-1 is 87% of the labels on staging. A pill reading "en" on every card
// crowds out the one that says something.
{
  const page = [
    profile("1", READER),
    ev("2", BOT, 1985, [["L", "ISO-639-1"], ["l", "en", "ISO-639-1"], ["p", READER]]),
    ev("3", BOT, 1985, [["L", "ugc"], ["l", "permaculture", "ugc"], ["p", READER]]),
  ];
  assert.deepStrictEqual(texts(pillsOn(page, "1")), ["permaculture"], "a language tag is furniture, not a reason");
  assert(QUIET_NAMESPACES.has("ISO-639-1"), "the demoted set is the named constant, not a condition in a branch");
}
// The namespace beside the VALUE decides, not the event's `L`: one label may
// carry several, and only the one written on this mark speaks for it.
{
  const page = [
    profile("1", READER),
    ev("2", BOT, 1985, [["L", "ISO-639-1"], ["L", "ugc"], ["l", "en", "ISO-639-1"], ["l", "zapped", "ugc"], ["p", READER]]),
  ];
  assert.deepStrictEqual(texts(pillsOn(page, "1")), ["zapped"], "a mixed label keeps the half that is provenance");
}

// ---- two tones, because two meanings --------------------------------------
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", BOT, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["p", READER]]),
  ];
  const pills = pillsOn(page, "1");
  assert.strictEqual(pills.find((p) => p.text === "Verified Human").gated, true,
    "a declaration reached the page past the trust gate — its presence IS the delegation");
  assert.strictEqual(pills.find((p) => p.text === "zapped").gated, false,
    "a NIP-32 label is ungated: anyone may publish one, and the card must not imply otherwise");
  assert.deepStrictEqual(texts(pills), ["Verified Human", "zapped"], "delegated sorts first");
}

// ---- a stable order, so a card does not reshuffle itself -------------------
{
  const page = [profile("1", READER)];
  for (const v of ["beta", "alpha", "gamma"]) {
    page.push(ev(v[0] + "f", BOT, 1985, [["L", "ugc"], ["l", v, "ugc"], ["p", READER]]));
  }
  assert.deepStrictEqual(texts(pillsOn(page, "1")), ["alpha", "beta", "gamma"],
    "equal counts break alphabetically, never on arrival order");
}

// ---- the three destinations -----------------------------------------------
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", SCORER, 30382, [["d", READER], ["rank", "92"]]),
    ev("4", SCORER, 30382, [["d", READER], ["t", "permaculture"]]),
    ev("5", BOT, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["p", READER]]),
  ];
  const by = Object.fromEntries(pillsOn(page, "1").map((p) => [p.text, p]));
  assert.deepStrictEqual([by["Verified Human"].to, by["Verified Human"].value], ["addr", `30392:${LISTER}:x`],
    "a list pill opens the list — the same address its own card opens");
  assert.strictEqual(by["rank 92"], undefined, "a score is not a reason — see below");
  assert.deepStrictEqual([by["permaculture"].to, by["permaculture"].value], ["topic", "permaculture"],
    "an assertion that carries topics pills the topic, and opens the topic");
  assert.deepStrictEqual([by["zapped"].to, by["zapped"].value], ["search", "zapped"],
    "a label runs a search for itself — the useful answer is the other events under it");
}

// ---- AN ASSERTION SPEAKS ONLY THROUGH ITS TOPICS ---------------------------
//
// It used to fall back to `rank 92`, or the word "scored" where there was no
// rank. A number out of its scale says nothing a reader can act on — `rank 2`
// beside `rank 98` invites a comparison the pill cannot support, since the
// scale is the service's and is nowhere on the card — and the page answers
// that question properly elsewhere, with the score chip on the face, which
// carries its lens with it. Measured on staging, the fallback was most of
// what this row drew about people.
{
  const scoreOnly = [profile("1", READER), ev("3", SCORER, 30382, [["d", READER], ["rank", "92"]])];
  assert.strictEqual(provenanceOf(scoreOnly, TRUSTED).size, 0, "a ranked assertion with no topic draws nothing at all");

  const bare = [profile("1", READER), ev("3", SCORER, 30382, [["d", READER]])];
  assert.strictEqual(provenanceOf(bare, TRUSTED).size, 0, "…and neither does one with no rank either");

  // EVERY `t`, not the first, and not only on the contact card: a topic means
  // the same thing whether the subject is a person, an event or an article.
  const many = pillsOn([profile("1", READER),
    ev("4", SCORER, 30382, [["d", READER], ["rank", "92"], ["t", "soil"], ["t", "permaculture"], ["t", "bitcoin"]])], "1");
  assert.deepStrictEqual(texts(many), ["bitcoin", "permaculture", "soil"],
    "all three topics, and the metric beside them still says nothing");

  const note = ev("1", READER, 1);
  assert.deepStrictEqual(texts(pillsOn([note, ev("5", SCORER, 30383, [["d", hex("1")], ["t", "howto"]])], "1")), ["howto"],
    "a 30383 about an event pills its topic too");
  const article = ev("1", READER, 30023, [["d", "post"]]);
  assert.deepStrictEqual(
    texts(provenanceOf([article, ev("6", SCORER, 30384, [["d", `30023:${READER}:post`], ["t", "essay"]])], TRUSTED).get(hex("1"))),
    ["essay"], "and so does a 30384 about an article");
}

// ---- the row is about the page, not the index -----------------------------
//
// A pointer whose target this page does not hold contributes nothing: a pill
// over an absent card is a claim about nothing.
{
  const page = [ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]])];
  assert.strictEqual(provenanceOf(page, TRUSTED).size, 0, "a list whose members are not on screen pills nothing");
}
// And a pointer never pills ITSELF — a list that is also a hit is the source.
{
  const page = [
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Self"], ["p", LISTER]]),
    profile("3", LISTER),
  ];
  assert.deepStrictEqual(texts(pillsOn(page, "3")), ["Self"], "the member profile gets the pill");
  assert.deepStrictEqual(pillsOn(page, "2"), [], "the list itself does not");
}

// ---- position is not attribution ------------------------------------------
//
// A subject used to arrive directly behind the pointer that named it, so
// reading the pair off neighbouring positions would have worked. It no longer
// does: the store places a spliced member by the confidence its list expressed
// about it, so a doubted member sinks past the organic hits between them. The
// pills must be identical however the page is ordered — this is the test that
// fails if anyone ever "optimizes" the two passes into a neighbour scan.
{
  const list = ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER], ["p", BOT]]);
  const label = ev("3", SCORER, 1985, [["L", "ugc"], ["l", "medical", "ugc"], ["e", hex("6")]]);
  const page = [list, profile("1", READER), profile("5", BOT), ev("6", BOT2, 1), label];
  const forward = provenanceOf(page, TRUSTED);
  const reversed = provenanceOf([...page].reverse(), TRUSTED);
  const shuffled = provenanceOf([page[3], page[1], page[4], page[0], page[2]], TRUSTED);
  for (const [id, label_] of [["1", "a member ahead of its list"], ["5", "a member far from its list"], ["6", "a labelled note"]]) {
    const want = texts(forward.get(hex(id)) || []);
    assert.ok(want.length, `${label_} must pill at all`);
    assert.deepStrictEqual(texts(reversed.get(hex(id)) || []), want, `${label_}: reversing the page changes nothing`);
    assert.deepStrictEqual(texts(shuffled.get(hex(id)) || []), want, `${label_}: shuffling the page changes nothing`);
  }
}

// ---- 30395 names no event, and 30385's subject is not one either ----------
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30395, [["d", "x"], ["title", "Known Books"], ["i", "isbn:9780316769488"]]),
    ev("3", SCORER, 30385, [["d", "isbn:9780316769488"], ["rank", "5"]]),
  ];
  assert.strictEqual(provenanceOf(page, TRUSTED).size, 0, "an external identifier is not an event this page can draw");
}

// ---- THE READER'S OWN CURATION SPEAKS -------------------------------------
//
// Since store `2bc79f5f40` a NIP-51 people list (30000) and a follow pack
// (39089) splice their members like a Trusted List does, so the row has to be
// able to name them. Before this the members arrived — the relay put them
// there — and the pill was silent, which is the row declining to give a reason
// the relay had.
//
// The staging report this came from is the shape: four lists titled exactly
// `Verified Human`, signed by humans the reader trusts, contributing nobody
// while a service-signed 30392 did all the splicing.
{
  // MINE, not a service's: these two kinds are gated on their signer like every
  // other declaration, and the signer that matters is the reader themselves.
  const mine = trusting(READER);
  const page = [
    profile("1", READER),
    ev("2", READER, 30000, [["d", "vh"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", READER, 39089, [["d", "pack"], ["title", "Soil People"], ["p", READER]]),
  ];
  const pills = provenanceOf(page, mine).get(hex("1"));
  assert.deepStrictEqual(texts(pills).sort(), ["Soil People", "Verified Human"],
    "a people list and a follow pack each name their members");
  assert.strictEqual(pills[0].to, "addr", "and a pill points at the list itself");
  assert.deepStrictEqual(pills.map((x) => x.value).sort(), [`30000:${READER}:vh`, `39089:${READER}:pack`],
    "…at its address, the same string the card's own permalink uses");
  assert.strictEqual(pills[0].gated, true,
    "gated: the relay would not have unpacked this list for anyone but its own reader");
}

// THE WORDS IT IS FINDABLE BY, which is not the same as any tag that looks
// like a name. A people list indexes `titleOrName()`, so an untitled one is
// reachable by its `name` and the pill has to draw that. A Trusted List
// indexes the title ALONE — reading `name` off one would put a word on a card
// that the relay never indexed and no reader could have searched for.
{
  // Signed by LISTER throughout, so one fixture's gate covers all three and
  // each case is only about which TAG the pill is read from. (A reader may
  // delegate these kinds deliberately; TRUSTED is what that looks like.)
  const named = [profile("1", READER), ev("2", LISTER, 30000, [["d", "x"], ["name", "Soil Nerds"], ["p", READER]])];
  assert.deepStrictEqual(texts(pillsOn(named, "1")), ["Soil Nerds"], "a 30000's `name` is indexed, so it is a pill");

  const list = [profile("1", READER), ev("2", LISTER, 30392, [["d", "x"], ["name", "Soil Nerds"], ["p", READER]])];
  assert.deepStrictEqual(texts(pillsOn(list, "1")), ["x"],
    "a 30392's `name` is not indexed: the pill falls back to `d`, which on that kind is a rounding error");

  const pack = [profile("1", READER), ev("2", LISTER, 39089, [["d", "y"], ["name", "Soil Nerds"], ["p", READER]])];
  assert.deepStrictEqual(texts(pillsOn(pack, "1")), [], "…and a follow pack indexes the title alone, so `name` alone says nothing");

  // The title still wins wherever there is one.
  const both = [profile("1", READER), ev("2", LISTER, 30000, [["d", "x"], ["title", "Titled"], ["name", "Named"], ["p", READER]])];
  assert.deepStrictEqual(texts(pillsOn(both, "1")), ["Titled"]);
}

// A STORAGE KEY IS NOT A REASON, and on the NIP-51 kinds it is most of the
// corpus. `d` is never indexed, so it is never a word anybody searched; for a
// 30392 the fallback is a rounding error because a delegated publisher titles
// what it computes, and for a 30000 it is 71% of the lists that name people.
//
// Measured on staging (2026-09-01), 400 kind-30000s: 183 name at least one
// person and only 53 carry a title or name. The other 130 would have put their
// storage keys on up to 10,934 member cards.
{
  const junk = ["intent-bloom-r0s63o3y-isPlaying", "chats/null/lastOpened", "nextblock.city/neighborhood", "communities", "dm-contacts"];
  for (const d of junk) {
    const page = [profile("1", READER), ev("2", READER, 30000, [["d", d], ["p", READER]])];
    assert.deepStrictEqual(texts(provenanceOf(page, trusting(READER)).get(hex("1")) || []), [],
      `an untitled people list says nothing rather than drawing ${JSON.stringify(d)} under a byline`);
  }
  // The row being PARTIAL is a property it already documents; being wrong is not.
  const titled = [profile("1", READER), ev("2", READER, 30000, [["d", "dm-contacts"], ["title", "DM Contacts"], ["p", READER]])];
  assert.deepStrictEqual(texts(provenanceOf(titled, trusting(READER)).get(hex("1"))), ["DM Contacts"],
    "…and the same list titled says its title");
}

// A MUTE LIST IS THE OPPOSITE OF A VOUCH. Quartz names the shape itself
// (`PeopleListEvent.BLOCK_LIST_D_TAG = "mute"`) and the relay splices it like
// any other people list — SearchReferences reads every public `p` and cannot
// tell a curation from a rejection. The row can: drawn, it is a positive chip
// on somebody the reader threw out.
//
// TITLED ONES ARE THE REASON THIS IS ITS OWN CHECK. Of 400 sampled, 41 are
// block-shaped and 9 name people publicly — the largest 3,980 of them — and
// two carry the title `Mute`, which the title rule above would happily draw.
{
  for (const d of ["mute", "block", "Blocked", "mutelists"]) {
    const bare = [profile("1", READER), ev("2", READER, 30000, [["d", d], ["p", READER]])];
    assert.deepStrictEqual(texts(provenanceOf(bare, trusting(READER)).get(hex("1")) || []), [],
      `a ${JSON.stringify(d)} list draws nothing`);
    const named = [profile("1", READER), ev("2", READER, 30000, [["d", d], ["title", "Mute"], ["p", READER]])];
    assert.deepStrictEqual(texts(provenanceOf(named, trusting(READER)).get(hex("1")) || []), [],
      `…and titling it does not turn it into provenance`);
  }
  // Not a substring rule: a genuine list is not silenced for containing the word.
  const real = [profile("1", READER), ev("2", READER, 30000, [["d", "muted-topics-i-follow"], ["title", "Unmuted"], ["p", READER]])];
  assert.deepStrictEqual(texts(provenanceOf(real, trusting(READER)).get(hex("1"))), ["Unmuted"],
    "a `d` that merely contains the word is a different list");
}

// A STRANGER'S LIST IS THE WHOLE SAFETY ARGUMENT. Anyone may title a list
// `bitcoin` and name a thousand accounts in it; what stops that is the same
// gate every other declaration passes — the reader is their own signer, and
// nobody else's list unpacks unless they deliberately delegated the kind.
{
  const STRANGER = hex("9");
  const page = [
    profile("1", READER),
    ev("2", READER, 30000, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", STRANGER, 30000, [["d", "y"], ["title", "Verified Human"], ["p", READER]]),
  ];
  const pills = provenanceOf(page, trusting(READER)).get(hex("1"));
  assert.strictEqual(pills.length, 1);
  assert.strictEqual(pills[0].count, 1, "a stranger's people list must not inflate the reader's own count");
  assert.deepStrictEqual(pills[0].authors, [READER]);
}

// ONLY THE PUBLIC HALF, and there is nothing to do about it here. NIP-51's
// private members are NIP-44 encrypted to the owner; the relay holds no signer
// either, so a member it could not read is a member it never spliced. What
// this pins is that the encrypted blob in `content` draws nothing — a pill off
// a member this page could not have been sent would be a claim about nothing.
{
  const secret = { ...ev("2", READER, 30000, [["d", "x"], ["title", "Private"], ["p", READER]]), content: "AAAA?iv=BBBB" };
  const page = [profile("1", READER), profile("4", LISTER), secret];
  const built = provenanceOf(page, trusting(READER));
  assert.deepStrictEqual(texts(built.get(hex("1"))), ["Private"], "the public member draws its pill");
  assert.strictEqual(built.has(hex("4")), false, "and nothing is invented for whoever the ciphertext names");
}

// ---- attribution: the face is drawn where it disambiguates -----------------
{
  const one = provenanceOf([
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "A"], ["p", READER]]),
    ev("3", LISTER, 30392, [["d", "y"], ["title", "B"], ["p", READER]]),
  ], TRUSTED);
  assert.strictEqual(facesNeeded(one), false,
    "one delegated publisher: a face on every gated pill would be the same face, forty times");
  const two = provenanceOf([
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "A"], ["p", READER]]),
    // A topic, because an assertion speaks only through those now — a bare
    // rank contributes nothing and so cannot be a second publisher.
    ev("3", SCORER, 30382, [["d", READER], ["t", "soil"]]),
  ], TRUSTED);
  assert.strictEqual(facesNeeded(two), true, "two publishers on one page: now the face says something");
  // Labels never enter the question — an ungated pill is attributed always,
  // and a page of nothing but labels must not switch the gated ones on.
  const labelsOnly = provenanceOf([profile("1", READER),
    ev("2", BOT, 1985, [["L", "ugc"], ["l", "a", "ugc"], ["p", READER]]),
    ev("3", BOT2, 1985, [["L", "ugc"], ["l", "b", "ugc"], ["p", READER]])], TRUSTED);
  assert.strictEqual(facesNeeded(labelsOnly), false, "label authors do not decide whether a GATED pill is attributed");
}

// ---- every target shape resolves -------------------------------------------
{
  const note = ev("1", READER, 1, []);
  const article = { ...ev("2", READER, 30023, [["d", "essay"]]) };
  const page = [
    note, article, profile("3", READER),
    ev("4", BOT, 1985, [["L", "ugc"], ["l", "on-note", "ugc"], ["e", hex("1")]]),
    ev("5", BOT, 1985, [["L", "ugc"], ["l", "on-addr", "ugc"], ["a", `30023:${READER}:essay`]]),
    ev("6", LISTER, 30393, [["d", "n"], ["title", "Worth Reading"], ["e", hex("1")]]),
    ev("7", LISTER, 30394, [["d", "a"], ["title", "Long Reads"], ["a", `30023:${READER}:essay`]]),
  ];
  assert.deepStrictEqual(texts(pillsOn(page, "1")).sort(), ["Worth Reading", "on-note"], "e targets resolve");
  assert.deepStrictEqual(texts(pillsOn(page, "2")).sort(), ["Long Reads", "on-addr"], "a targets resolve");
  // An `r`/`t` label target names a url and a topic — neither is an event this
  // page draws, and reading them would invent a card that does not exist.
  const urlLabel = provenanceOf([note, ev("8", BOT, 1985, [["L", "ugc"], ["l", "x", "ugc"], ["r", "https://example.com"]])], TRUSTED);
  assert.strictEqual(urlLabel.size, 0, "a url target is not a record");
}

// ---- a page with nothing to say says nothing -------------------------------
assert.strictEqual(provenanceOf([], TRUSTED).size, 0, "an empty page");
assert.strictEqual(provenanceOf(null, TRUSTED).size, 0, "no page at all");
assert.strictEqual(provenanceOf([{ kind: 1 }, null, { id: "nope", kind: 1985, tags: [] }], TRUSTED).size, 0,
  "a malformed event is skipped rather than thrown over");

// ---- the page's own state -------------------------------------------------
//
// ---- WHOSE WORD, not just which kind ---------------------------------------
//
// The gate the relay used to be trusted for, and it was never quite true even
// of the relay: its expansion COMPANION is gated, but plain recall is not —
// "an explicit `kinds:[30392]` is a NIP-01 ask and serves strangers' lists as
// plain hits, gate or no gate" — and a search naming no kinds recalls every
// kind. So a stranger's list whose TITLE matches the search text lands in the
// answer beside the delegated publisher's, and it used to be drawn as vouched.
//
// Worse than a wrong tone: it collapsed by value into the delegated pill and
// took the COUNT to 2. Anyone could inflate a publisher's corroboration
// number by signing a list with the same title.
{
  const STRANGER = hex("9");
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", STRANGER, 30392, [["d", "y"], ["title", "Verified Human"], ["p", READER]]),
  ];
  const pills = provenanceOf(page, trusting(LISTER)).get(hex("1"));
  assert.strictEqual(pills.length, 1);
  assert.strictEqual(pills[0].count, 1, "a stranger's list must not inflate a delegated publisher's count");
  assert.deepStrictEqual(pills[0].authors, [LISTER], "…nor ride along as one of its authors");

  // Not a quieter pill — nothing. The row says why a card is in this page, and
  // the relay would not unpack a stranger's list for this reader, so whatever
  // put the profile here, it was not that.
  const alone = provenanceOf([profile("1", READER),
    ev("3", STRANGER, 30392, [["d", "y"], ["title", "Verified Human"], ["p", READER]])], trusting(LISTER));
  assert.strictEqual(alone.size, 0, "an undelegated list contributes nothing at all");
}

// THE SERVICE KEY FOR THE KIND, PLUS THE OBSERVER. The store fetches
// declarations "from their enrolled signers only, plus the reader", so a
// reader's own Trusted List unpacks and splices its members — and the row has
// to be able to say so, or the relay put a profile on the page for a reason
// the row then declined to give.
{
  const mine = provenanceOf([profile("1", READER),
    ev("2", READER, 30392, [["d", "x"], ["title", "Mine"], ["p", READER]])], trusting(LISTER, READER));
  assert.deepStrictEqual(texts(mine.get(hex("1"))), ["Mine"], "a reader's own list speaks for itself");
}

// NO DECLARATION KIND HAS AN UNGATED PATH — the invariant, walked rather than
// argued, because the gate is one line in `contributionsOf` and a kind added
// past it would be a silent hole. Every kind in DECLARATION_KINDS, a stranger
// signing it, every member and subject tag it could use aimed at things this
// page holds: nothing, on all of them. And the stranger never rides in as an
// AUTHOR of the pill a delegated signer earned, which is the subtler half —
// pills collapse by value, so a stranger titling a list like yours would
// otherwise be attributed behind your own.
{
  const { DECLARATION_KINDS } = P;
  const STRANGER = hex("9");
  const SUBJ = hex("1");
  const MEMBER_TAGS = { 30000: "p", 30392: "p", 30393: "e", 30394: "a", 39089: "p" };
  const article = ev("2", SUBJ, 30023, [["d", "x"]]);
  const page = [profile("1", SUBJ), article];
  const addr = `30023:${SUBJ}:x`;
  assert.ok(DECLARATION_KINDS.size >= 10, "the walk is over the real set, not a stale copy of it");
  for (const kind of DECLARATION_KINDS) {
    const member = MEMBER_TAGS[kind];
    // A declaration carrying EVERY shape at once: a list's member tag, an
    // assertion's `d` subject, and a topic — so no kind escapes for want of
    // the tag it happens to read.
    const tags = [
      ["d", kind >= 30382 && kind <= 30385 ? (kind === 30383 ? hex("1") : kind === 30384 ? addr : SUBJ) : "x"],
      ["title", "Verified Human"], ["name", "Verified Human"], ["t", "vh"],
      ...(member ? [[member, member === "p" ? SUBJ : member === "e" ? hex("1") : addr]] : []),
    ];
    const stranger = ev("3", STRANGER, kind, tags);
    assert.strictEqual(provenanceOf([...page, stranger], TRUSTED).size, 0,
      `kind ${kind}: a signer this reader never delegated contributes nothing`);

    const mine = ev("4", LISTER, kind, tags);
    const both = provenanceOf([...page, stranger, mine], TRUSTED);
    for (const pills of both.values()) {
      for (const pill of pills) {
        assert.ok(!pill.authors.includes(STRANGER), `kind ${kind}: a stranger must not be attributed behind a delegated pill`);
        assert.strictEqual(pill.count, 1, `kind ${kind}: …nor inflate its count`);
      }
    }
  }
}

// ABSENT MEANS NOBODY. An anonymous reader delegates no one and gets label
// pills only — exactly what the relay serves them.
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]]),
    ev("3", BOT, 1985, [["L", "ugc"], ["l", "zapped", "ugc"], ["p", READER]]),
  ];
  assert.deepStrictEqual(texts(provenanceOf(page, undefined).get(hex("1"))), ["zapped"],
    "no Map: the declaration is dropped and NIP-32, which is open by construction, is not");
  assert.deepStrictEqual(texts(provenanceOf(page, new Map()).get(hex("1"))), ["zapped"]);
}

// seedProvenance REPLACES rather than merges: a second search must not leave
// the first one's pills on a card that survived into both.
{
  const { seedProvenance, forgetProvenance, provenance, attribution, provenanceEpoch } = P;
  const page = [profile("1", READER), ev("2", LISTER, 30392, [["d", "x"], ["title", "VH"], ["p", READER]])];
  assert.strictEqual(seedProvenance(page, TRUSTED), 1, "one card gained a row");
  assert.strictEqual(provenance.size, 1);
  assert.strictEqual(seedProvenance([profile("1", READER)], TRUSTED), 0, "a page with no pointers clears the last page's");
  assert.strictEqual(provenance.size, 0, "and leaves nothing behind");

  // A permalink clears on the way in — forgetProvenance is what keeps the row
  // from arriving there as a leftover of the search behind it.
  seedProvenance([...page, ev("3", SCORER, 30382, [["d", READER], ["t", "soil"]])], TRUSTED);
  assert.strictEqual(attribution.faces, true, "two publishers, so gated pills are attributed");
  forgetProvenance();
  assert.strictEqual(provenance.size, 0, "cleared");
  assert.strictEqual(attribution.faces, false, "and the attribution flag goes with it, not just the pills");
}

// A TRENDING FEED IS NOT PROVENANCE, and it is the shape that reaches PEOPLE.
// Ditto publishes one as NIP-32: the value is `#p` — a NIP-01 tag name saying
// "this list is about pubkeys" — and one event names forty of them, so a
// single post would put a pill reading `#p` on forty cards. Measured on
// staging, it was 100% of the labels about one page of profiles.
{
  const trend = ev("7", BOT, 1985, [
    ["L", "pub.ditto.trends"], ["l", "#p", "pub.ditto.trends"],
    ["p", READER, "wss://relay.ditto.pub/", "432", "1103"],
  ]);
  const real = ev("6", BOT2, 1985, [["L", "ugc"], ["l", "spammer", "ugc"], ["p", READER]]);
  assert.deepStrictEqual(texts(provenanceOf([profile("1", READER), trend, real], TRUSTED).get(hex("1"))), ["spammer"],
    "the trend label is furniture; the one beside it that says something is not");
}

// EVERY WRITE MOVES THE EPOCH, a clear included — which is the half a counter
// living in the caller could not do. The row is filled in two passes now, and
// between them the reader can start another search OR open a permalink;
// entity.js clears the row on the way into one, and a late second pass that
// only watched for re-SEEDS would write it straight back, restoring exactly
// the "how you got here" reading the clear exists to prevent.
{
  const { seedProvenance, forgetProvenance, provenanceEpoch } = P;
  const page = [profile("1", READER), ev("2", LISTER, 30392, [["d", "x"], ["title", "VH"], ["p", READER]])];
  const before = provenanceEpoch();
  seedProvenance(page, TRUSTED);
  const seeded = provenanceEpoch();
  assert.notStrictEqual(seeded, before, "a seed moves the epoch");
  forgetProvenance();
  assert.notStrictEqual(provenanceEpoch(), seeded, "and so does a clear — the case a caller-side counter missed");
}

console.log("provenance: collapse, demotion, tones, order, order-independence, destinations, targets, the reader's own lists and attribution — all assertions passed");
