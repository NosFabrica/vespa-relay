# Web test decisions (sync card, verdicts panel, cards, provenance row)

What `sync.test.mjs`, `verdicts.test.mjs`, `cards.test.mjs` and
`provenance.test.mjs` used to carry in their comments and no other decisions
file records. The sync card's judgements are in `web-shared-sync.md`, the
provenance rules in `web-search.md`, the verdict tags on the Kotlin side in
`peers.md` and `fitness.md`, and the card helpers in `web-shared-helpers.md`.

**The pool words are read out of `VisitPool.kt`, not copied.** `VisitPoolTest`
binds the router's `POOL_` words to the document's glossary, and nothing bound
them to `shared/sync.js`, which draws the tables: a rename in Kotlin passed the
whole suite and emptied a panel on the page with no failure in either language.
The suite reads the `const val` literals from the source, so a renamed or added
pool word fails in the file that draws it.

**Free text off the wire never indexes a plain object.** A bottleneck word, a
reason and a hostname are all used as keys for a tone or label lookup. A relay
publishing `constructor` or `__proto__` reached `Object.prototype`, got a
function back, and a destructuring of it threw the whole render away, which is
worse than the unknown word it came from.

**Every verdict on the panel expires on both the TTL and the rules epoch, the
cleared form and the grade included.** The same omission shipped three times:
`unstable` counted every `self-consistent: false` ever written, a retired
cleared verdict fell out of every counter (not folded, not cleared, not expired,
not silent), and a graded url counted as silent because `silent` tested only
the fold and the stability tag. Each drew the panel disagreeing with the
router's own `current` on exactly the relays it was re-dialling.

**The survivor is inferred from the folds that point at it, with every field a
read record has.** `RelayAliases.learn` clears only a leader nothing folded
onto, so the url a group collapses to is the one with no `same-as` to find;
read purely from records, `articles.layer3.news` drew as 23 urls and 0 dialled
for a host dialled exactly once. The synthesised row once omitted the
collection fields, and the panel drew thousands of rows correctly, died on the
first inferred survivor inside a `for…of` in the renderer, and left its own
filter hidden with no error on screen.

**The record walk grows its page across a same-second run, and a cut page ends
it as partial.** quartz's monitor stamps a flushed batch with one `created_at`,
and a fixed page can never span a run longer than itself: it returns the same
page, the cursor cannot move, and stepping below the boundary skips the rest.
Measured on the live store as 4,595 of 5,296 records read, reported complete.
A page `Relay.reqOnce` cut on timeout is marked `complete: false`; read as an
empty page it meant "the store is exhausted" on precisely the loaded relay the
timeout exists for.

**The metadata other writers put on a 30166 is drawn so a clobbering writer is
visible.** quartz's passive monitor writes `n`, `rtt-*` and `R` onto the same
record the fold writes `same-as` onto, and `RelayAliasRecord.edit` has to
carry every tag it does not own. A writer that rebuilt the record was measured
once turning `[d, n, rtt-open]` into `[d, same-as]`, a valid signed record
that says less; the panel reads those tags and counts unknown ones so the
regression shows in production rather than only in a unit test.

**Trailing whitespace is not trimmed with `/\s+$/`.** The regex retries every
start position inside a run of whitespace, so 80,000 spaces in the middle of a
line of a stranger's kind 1337 cost about five seconds on the main thread per
card. Code is clipped by lines, which also keeps the indentation `clip()` would
have trimmed off every line.

**An address that cannot be encoded falls through to the note id.** `naddr`'s
TLV length prefix is one byte, so a `d` over 255 UTF-8 bytes has no legal
encoding on a well-formed tag. The addressable branch returned that null
straight through and the card had no link at all, not its date, not its body,
and `openPicked` silently did nothing with the event id sitting unused.

**The escaping claim is asserted with one short payload, verbatim, in three
places.** `fmtTs(tagOf(ev, …))` hands its argument back when it is not a
number, so `["endsAt", "<img …>"]` on a kind 1068 executed while the rest of
the card escaped; `dim` reaches a style attribute; the reply line validates
its ids before escaping and is poisoned in the slots that survive validation.
The payload `"><b BAD>` is short so no clip truncates it, and is asserted
absent verbatim, since `onerror=` survives correct escaping inertly and would
fail on right code.

**The type-ahead row draws no provenance, and app.js depends on it.** The popup
is drawn over the results list with both on screen, so its hydrate runs with
`row: "keep"` and must not touch the shared provenance map. That held silently
until a debounced keystroke was found taking a search's whole row down with it.
