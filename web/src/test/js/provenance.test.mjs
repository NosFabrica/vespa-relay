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
const pillsOn = (page, id) => provenanceOf(page).get(hex(id)) || [];
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
  assert.strictEqual(by["rank 92"].to, "addr", "a score with no topic opens the card that made it");
  assert.deepStrictEqual([by["permaculture"].to, by["permaculture"].value], ["topic", "permaculture"],
    "a contact card that carries topics pills the topic, and opens the topic");
  assert.deepStrictEqual([by["zapped"].to, by["zapped"].value], ["search", "zapped"],
    "a label runs a search for itself — the useful answer is the other events under it");
  // The two shapes of a contact card are exclusive: a card with topics says
  // the topics, not the metric, because the topic is why it matched.
  const topicOnly = pillsOn([profile("1", READER), ev("4", SCORER, 30382, [["d", READER], ["rank", "92"], ["t", "soil"]])], "1");
  assert.deepStrictEqual(texts(topicOnly), ["soil"], "a topic beats the metric on the same card");
}

// ---- the row is about the page, not the index -----------------------------
//
// A pointer whose target this page does not hold contributes nothing: a pill
// over an absent card is a claim about nothing.
{
  const page = [ev("2", LISTER, 30392, [["d", "x"], ["title", "Verified Human"], ["p", READER]])];
  assert.strictEqual(provenanceOf(page).size, 0, "a list whose members are not on screen pills nothing");
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

// ---- 30395 names no event, and 30385's subject is not one either ----------
{
  const page = [
    profile("1", READER),
    ev("2", LISTER, 30395, [["d", "x"], ["title", "Known Books"], ["i", "isbn:9780316769488"]]),
    ev("3", SCORER, 30385, [["d", "isbn:9780316769488"], ["rank", "5"]]),
  ];
  assert.strictEqual(provenanceOf(page).size, 0, "an external identifier is not an event this page can draw");
}

// ---- attribution: the face is drawn where it disambiguates -----------------
{
  const one = provenanceOf([
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "A"], ["p", READER]]),
    ev("3", LISTER, 30392, [["d", "y"], ["title", "B"], ["p", READER]]),
  ]);
  assert.strictEqual(facesNeeded(one), false,
    "one delegated publisher: a face on every gated pill would be the same face, forty times");
  const two = provenanceOf([
    profile("1", READER),
    ev("2", LISTER, 30392, [["d", "x"], ["title", "A"], ["p", READER]]),
    ev("3", SCORER, 30382, [["d", READER], ["rank", "9"]]),
  ]);
  assert.strictEqual(facesNeeded(two), true, "two publishers on one page: now the face says something");
  // Labels never enter the question — an ungated pill is attributed always,
  // and a page of nothing but labels must not switch the gated ones on.
  const labelsOnly = provenanceOf([profile("1", READER),
    ev("2", BOT, 1985, [["L", "ugc"], ["l", "a", "ugc"], ["p", READER]]),
    ev("3", BOT2, 1985, [["L", "ugc"], ["l", "b", "ugc"], ["p", READER]])]);
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
  const urlLabel = provenanceOf([note, ev("8", BOT, 1985, [["L", "ugc"], ["l", "x", "ugc"], ["r", "https://example.com"]])]);
  assert.strictEqual(urlLabel.size, 0, "a url target is not a record");
}

// ---- a page with nothing to say says nothing -------------------------------
assert.strictEqual(provenanceOf([]).size, 0, "an empty page");
assert.strictEqual(provenanceOf(null).size, 0, "no page at all");
assert.strictEqual(provenanceOf([{ kind: 1 }, null, { id: "nope", kind: 1985, tags: [] }]).size, 0,
  "a malformed event is skipped rather than thrown over");

console.log("provenance: collapse, demotion, tones, order, destinations, targets and attribution — all assertions passed");
