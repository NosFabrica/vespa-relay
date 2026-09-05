# Shared page helper decisions

The history behind the small modules in `web/src/main/resources/web/shared/`
(`pointers.js`, `groups.js`, `groupnames.js`, `nip19.js`, `parents.js`,
`page.js`, `calendar.js`, `format.js`, `kinds.js`, `asks.js`, `profiles.js`,
`nip05.js`, `keynav.js`, `lens.js`, `avatar.js`), moved out of the source so
the code reads on its own. One paragraph per decision; `git log -L` on the
function finds the commit.

## groups.js and groupnames.js

**A 10009 row and a 39000 row are never merged.** A group's identity is the
pair (id, host relay), as quartz's `GroupId` keys it. A `group` tag names the
host as a url and a kind 39000 names it as the pubkey that signed it, and
nothing in this store joins a relay's key to its url, so folding two rows that
share an id would print one host's url under another host's name. They stay
two rows, each saying where it came from, and both are marked `ambiguous`
because an `h` filter carries only the bare id either way.

**The relay's rows are not re-filtered by substring.** The picker once ran
every kind-39000 hit through the same `includes` test it applies to the
reader's own list, which threw away every match the index can make and a
substring cannot: an `about` mentioning bitcoin returned no rows, and "alices"
against "Alice's Club" returned none. The relay's ranking is kept in its own
order and its whole.

**A group name lookup runs on the anonymous connection.** The picker asks the
authenticated socket, because which groups are yours is a question about the
reader. A name for an id the reader already typed is a fact about a subject,
and the store applies the observer as a filter, so asking there left a reader
with no scores mirrored here staring at the hex id.

**`HOSTS_PER_ID` is eight.** One id signed by two relays is the case the module
exists to notice, so the lookup's limit leaves room for several hosts per id.
It is a bound rather than a guarantee; an unbounded read on every render was a
real cost against a guessed one.

## nip19.js

**nevent is minted, with hints.** It was left unminted on the grounds that its
TLV is hints the page had nothing to put in. A reply's `e` tag carries the
relay its author believed holds the parent and often the parent's pubkey, and
the entity page dials an identifier's hints when the index misses; a `note1`
has none to dial, which was the difference between "in reply to Alice" opening
her post and opening "Not here".

**An encoder returns "" for a bad id rather than throwing.** Cards render events
fetched from strangers' relays, and `noteId(ev.id)` on an event with no id took
the whole permalink down. A missing identifier costs that one link.

**`tinyNpub` is prefix-only.** The head-and-tail form is 19 characters, which
does not fit a grid cell, and the CSS then ellipsed it a second time so the
label read `npub1eedm57z…ag…`. Six characters after `npub1` tell two nameless
strangers apart and the whole key is in the title.

## parents.js and profiles.js

**A missing answer is cached only after EOSE.** `req()` resolves with whatever
arrived when its timeout fires. Profiles once recorded `null` on any failure,
so one dropped lookup left a pubkey faceless for the session, which is why
signing in appeared to need a page refresh: the first attempt poisoned the
entry for the reader's own key. The same rule holds for reply parents and
group names.

**`displayName` prefers `display_name`.** Three call sites had the order the
other way round and disagreed with the rest of the page about what somebody is
called, so there is one function.

**A kind 42's lone `e` tag is the room, not a parent.** A NIP-28 channel
message carries the channel as `["e", <kind 40 id>, …, "root"]`, and taking it
as the parent drew "in reply to whoever opened the channel" over every line of
chat.

## page.js

**The compact formatter is hoisted.** `toLocaleString(undefined, opts)` builds a
fresh `Intl.NumberFormat` per call, and options defeat the engine's cache for
the argument-less form: over 20,000 calls in Chromium, 429ms inline against
12ms through one kept instance, where bare `toLocaleString()` is 12ms either
way.

**Tiles are sized per row, not per tile.** Sizing each tile to its own text
left "1,882,401" a third smaller than the "27" beside it, which reads as the
small number being the important one. One `--len` on the row, set by the value
with the least room, and every `1fr` tile resolves the same size.

**A non-ok status always gets the loud badge.** The ternary once read
"partial ? loud : quiet", so `failed` drew quieter than `partial`.

## calendar.js

**The locale formatters are built once.** `toLocaleDateString` builds a fresh
formatter on every call, measured at 0.077ms against 0.0015ms for a kept one,
times the 46 calls a calendar render makes: 3.5ms of the 6.4ms an arrow-key
repeat cost, most of a frame per keypress.

**Days are stepped by local date fields.** Adding 86,400,000 lands an hour out
on the two days a year the clocks move, which turned "last 7 days" into six
days and 23 hours.

## format.js

**`firstTag` is total over the event.** entity.js renders what a hinted relay
returns before handing it over for verification, and this line once iterated
`ev.tags` bare while base.js guarded, so an event with no tags array threw in
70 of the 118 renderers, outside `showEntity`'s try/catch: the page stopped at
its skeleton with the loading line still on it.

**An opaque `d` is not a title.** Amethyst's kind 22 short videos carry
`d f56d739a-09c9-4f0b-ba82-f8c21e1a6b8e`, and that UUID led every one of those
cards while the caption sat in `content`. A UUID, a hex blob and a bare unix
timestamp fall through to whatever the card would say next.

**A markdown body is reduced to text, never rendered.** The preview for "On
Relays, Bandwidth, and Who Pays" opened with `## Somebody is paying for this`
and spent its lines on `1. **Paid relays.**`. The excerpt drops what a preview
cannot show and unwraps the rest; it emits no markup, so the call site's escape
still covers it. The first prose run is taken because a kept heading runs
straight into the sentence under it.

## kinds.js

**Every kind with a renderer is named and tinted.** The badge is the only part
of a card that says what it is looking at, so a registered kind falling through
to "kind 30003" is a card that draws its contents and refuses to say what they
are. apps.js had renderers but no tint, and its four kinds were the only dressed
cards still wearing the unknown-kind grey. The operator pages once kept their
own short list of supported kinds, a second copy nothing kept honest, so
`KNOWN_KINDS` is read off the same table.

## asks.js

**The relay's cost is the match set, not the page.** Against staging
(2026-09-03, `bitcoin`, kind 1) `limit: 1` answered in 4.4s and `limit: 200` in
4.0s, and the page made exactly that pair on every search: the type-ahead's
eight rows and then the results view's first page. The views agree on one width
and the second gets the first one's promise.

**`ASK_FRESH_MS` is sixty seconds.** A keystroke-to-Enter gap with room to
spare, and far under the pager's own notion of stale, since it re-asks at a
wider limit on every page turn.

## nip05.js

**The watcher disconnects before observing.** Every caller runs just after a
container's innerHTML is replaced, and an IntersectionObserver holds a strong
reference to each target until it is unobserved, which only happened when a
target scrolled into view. A session of forty searches kept every nip05 element
of all forty alive, none in the document.

**Unreachable is not invalid.** A domain that is offline, sends no CORS header
or has bad TLS is not a failed claim; marking it so accused people of lying
because their web server was down.

## keynav.js

**Typing is detected by `isContentEditable`, not the tag list.** The search box
is a div, so a tag test read the `j` somebody typed into their query as a move
and ate it; the "/" shortcut had already been bitten the same way.

**The results cursor clamps where the popup wraps.** The popup is eight rows in
one box, all on screen, so a wrap is a move the eye follows. A results list is
forty cards over several screens, and wrapping teleported the reader past
everything they were walking through.

## avatar.js

**One `avatarHtml`, with size as an argument.** Four modules had grown their own
face markup; they differed only in how big the face was and in which of the
three parts (picture, generated fallback, score chip) they had forgotten.

**The pulse page polls every two seconds.** `/pulse.json` is a read of in-process counters,
not a rollup of Vespa queries, so it costs the relay nothing comparable to the stats page's
thirty-second floor, and a thirty-second window smooths away the spike an operator opened the
page to see. `shared/pulse.js` differences cumulative counters into rates over that window.

**Rates are measured on the server's own clock, never the browser's.** `windowOf` takes the
window between two documents from `uptimeSeconds`; a reader whose clock is minutes off would
otherwise see every rate scaled by the error. Uptime going backwards means the process
restarted and every counter with it, so the baseline is dropped rather than differenced into a
large negative rate.

**Gauges are never differenced, and have their own accessor and panel.** A queue depth is not
a rate and "total ever queued" answers nothing, so `gaugesOf` is kept apart from every counter
accessor and the gauges are drawn in their own panel, never mixed into the rate strip, so the
distinction survives a refactor.

**A lock wait is charged to its first holder.** Over a long wait the store mutex may change
hands several times; all of that wait is attributed to whoever held it when the waiter
arrived, and the page states that rather than implying a per-holder split.

**Client sections are gated on the document's own flag.** `showsClients` reads
`clientDerived`, not whether the observer, search-term and slow-read arrays are present: a
build that serves no client sections and a relay nobody has searched yet both produce empty
arrays, and only the flag tells them apart.

**The slow-read column shows the predicate.** Every YQL the store emits opens with the same
projection (`select id, pubkey, created_at, ... from event where ...`), so a column of them
truncated to forty characters read identically; `whereOf` cuts at ` where ` and the whole
statement stays on the row's tooltip. A shape with no `where` is shown untouched rather than blank.

**`DOMINANT_SHARE` is strictly past one half, and `CHATTY_CALLS_PER_DOC` is four.** An even
two-way split is exactly 0.5 and has no dominant half, so naming one there would be a sentence
the table contradicts. One engine call per document is the floor for a read that returns what
it asked for and two is a probe plus a write, so four is the store's own "never ingest in a
loop over insert()" contract as a number.
