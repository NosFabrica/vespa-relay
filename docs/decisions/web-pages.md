# Web page decisions

The history behind `web/src/main/resources/index.html`, `stats.html` and
`observer_stats.html`, moved out of the markup so the comments can stay short.
One paragraph per decision; `git log -L` on the block finds the commit.

## index.html

**The favicon is not the brand mark verbatim.** The mark is drawn in
`currentColor`, and a tab strip has no `currentColor` and no single background,
so no one ink survives both themes on a transparent icon; the tile fixes the
ground and the ink is white in either. The mark's 1.7-unit stroke is 0.9 device
pixels at 16px and antialiases into a smudge, so the icon's is 2.6 with less
margin. RelayFaviconTest compares the mark's path `d` against favicon.svg by
string, which is why the third link is spelled as an endpoint rather than `h2.8`.

**Relative asset paths do not make the search page prefix-mountable.** The page
is a SPA whose history writes are anchored at the root (`currentUrl` returns
`"/" + qs`), so a prefix would survive the first load and be lost by the first
navigation. stats.html is safe under a prefix because its imports are in an
inline module and it never rewrites its own url.

**Module preload hints live in the head, ahead of the stylesheet.** Without
them the browser discovers the import graph a wave at a time (index.html, then
app.js, then its seven imports, then the four behind those), each wave a round
trip: 188ms on localhost, 1,513ms measured over a Cloudflare tunnel. The hints
once sat at the end of the body, after 530 lines of CSS, which delayed the
fetches they exist to start. The preload crawler drops the entry module, so
app.js's own `<script type="module">` sits up there too; deferred by definition,
it still runs after the DOM is parsed. `preload.test.mjs` holds the list to the
real graph.

**Every read names its lens.** The relay refuses an unauthenticated REQ with
`auth-required:` unless it carries a NIP-50 `observer:` or waives a lens with
`include:spam`. Signed in, the socket's identity is the answer. Signed out,
`web/shared/lens.js` is the one place that decides which token to send:
`observer:` when somebody is picked in "ranking as", `include:spam` otherwise.
"Ranking as" works signed out because scores are public and a lens needs no
signature; it reads "nobody" rather than "me" when there is no me.

**Signing in does not enrol the reader.** The page header and app.js both
claimed it did while the relay's enrolment hook was wired to nothing. The store
treats a lens as a filter, so a reader whose trust chain has not been mirrored
gets an empty ranked search, and nothing on screen could tell that from a broken
relay. `web/readiness.js` is the panel that says which link is missing (relay
list, kind 10040, provider scores), how far the import has got when a
denominator exists, and, for a relay that has never seen the reader's kind
10002, a field to name a relay to copy the lists from.

**One control decides whose trust ranks the page.** "Ranking as" replaced a
login toggle beside a lens picker, a pair whose off-plus-somebody combination
was legal and did nothing. Only pubkeys that published a kind 10040 are offered
(271 against 12.28M profiles at the time), since only their scores are projected.

**The three advanced filters sit behind one disclosure with a count.** Spread
across the bar they were permanent furniture for the few searches that use
them, and on a phone they pushed the kind chips into a sideways scroll. The
badge is what the hiding owes back: `?sort=`, `?spam=1` and `?as=` mean a
shared link can arrive with all three set, and a filter nothing admits to is
worse than a crowded bar. `filters.test.mjs` asserts the badge hides at zero.

**The syntax button is an unlabelled mark.** The bar is 748px at the page's
widest, the kind chips take about 598 of it, and a labelled button wrapped the
row, costing a permanent 38px off every results page once the bar is sticky.
The mark fits with nothing to spare; a ninth chip wraps the row the way a phone
already does, which is a line and not a broken control.

**The zero-dependency rule is a rule.** The relay client, the bech32 codec and
the rendering are hand-rolled; adding an npm library is a constitutional change.
The page was one file until per-kind entity renderers became the plan; a
registry of card modules holds what one file could not. `cards.test.mjs` fails
when a kind registers a renderer without a fixture, a badge label or a family
tone, which is how a bookmark set was found rendering under "kind 30003".

**A reply links its parent event, not the author's profile.** A reader
following "in reply to" wants the thing that was said. Which `e` tag is the
parent is NIP-10's rule in `web/shared/parents.js`; the author is a tag hint
when the client left one and a lookup by id when not, so the line can fill in
after the card. It replaced a props row reading `note1qqq…`.

**A card opens on click through `data-href`, and the date is a real anchor.**
The hover lift had promised a click since it was written while only inner
links navigated. app.js yields to any real control and to a text selection;
the byline date carries the same destination as an ordinary `<a>`, so
middle-click, copy-link and Tab work. Together they let the `note:` and `id:`
props rows go.

**Keyboard navigation asks one function whether a key is typed into the box.**
The search box is a contenteditable div, so a tag-name test alone eats a `j` or
`/` typed as text; that trap caught "/" first. Enter belongs to whatever has
the focus ring when it is a chip, a link or the json toggle, since Tab-then-Enter
predates the cursor. The cursor tracks the event rather than the row, because
names and reply parents land after the cards and repaint them. Submitting a
search blurs the field, which is what makes the keys reachable at all.

**`since:` and `until:` are ISO and inclusive.** `06/08/2026` is two days
depending on the reader; the pill respells the ISO token their way. `since` is
00:00 of its day and `until` 23:59 of its own in the reader's timezone, because
NIP-01's `until` is inclusive and a window from a day to itself must be a day
wide. The calendar and the people picker share one popup box and are never open
together: one caret is inside one token.

**A hashtag pill forms only when the caret leaves the tag.** A hashtag is a
token one character in, and a pill formed at `#n` would re-render the field on
every keystroke.

**A hashtag search is a union of three conventions in one REQ.** `#t` for the
events that tag the topic, kind 1111 with the NIP-73 id in `i`/`I` for the
comments written on it (no `t` at all), and the NIP-32 label `#l` for events
labelled with it. The label filter asks the value alone: NIP-32 only recommends
the `L` namespace, and the vocabulary mark is the tag's second value, which
NIP-01 does not index. Tag values compare cased in the engine, so the ask
carries `nostr`, `Nostr` and `NOSTR`; `#I` must stay uppercase through Quartz's
REQ parse. `limit` is per filter, so the side filters ride at a quarter of the
view's. The store merges filters that share a rank profile into one order, which
is what makes the quarter a budget rather than a tail cut. Pinned by
RelayProtocolTest "the search page's hashtag union" and "a ranked union is
served as one order".

**NIP-73 scopes keep kind 1111 whatever the tab says.** `site:`, `isbn:`,
`geo:`, `isan:`, `doi:` and the `podcast:` prefixes narrow to the comments
written on that thing: `#I` for threads rooted at it at the full limit, `#i` for
the odd event whose parent it is at a side limit. Gating them off with the tab
would leave the token visible in the box and inert in the REQ. The ask carries
the canonical spelling and the typed one, since NIP-73 normalizes per family and
commenters do not reliably comply. No picker: a url or an isbn is pasted.

**`group:` gets a picker because its ids are opaque.** A host relay minted the
id, so it is what nobody can type from memory; the rows offer the name and a
pick writes the id. The read is on the authenticated socket, so a reader the
relay cannot rank sees no personal groups, and the readiness panel is what says
why. A group is the pair (id, host) but an `h` tag carries only the id, and the
two sources name a host incompatibly (a 10009 `group` tag gives a url, a 39000
gives the signing pubkey); `shared/groups.js` keeps them as separate rows and
flags any id more than one row carries rather than inventing the join.

**Private groups are decrypted on use, once, and never re-asked unprompted.**
A 10009 may carry NIP-44 items self-encrypted in `.content`. The page asks the
extension at the moment `group:` is used, not on load; no payload means no ask;
one in-flight promise serves every keystroke; and a refusal is final until the
reader clicks "Unlock your private groups". The ask is not awaited, so the
public rows draw immediately. An empty private list encrypts the empty string,
not `[]`, so a payload is not evidence of contents. Nothing records the scheme,
so `isNip04` ports quartz's shape test byte for byte (`?iv=` exactly 28 from the
end, after a `-null` strip for a client bug that shipped).

**The landing feed is an empty search and is drawn only signed in.** With no
words, sort, spam toggle or lens the filter builder leaves `{kinds, limit}`, a
plain NIP-01 read answered newest-first; the trust gate comes from the
connection. Signed out the same read is the whole mirror in time order, which
the hero should not hand anybody unasked. The feed hides the Filters panel
because those three cannot ride on an ask with no search string; the kind chips
stay, and a chip replaces the feed's content kinds rather than intersecting
them, since four of the seven narrowing chips share no kind with the default.

**The URL is the search; the type-ahead never touches it.** Before this the
page held its state in JS only: Back left the site from the middle of a search,
reload emptied it, and a result list could not be handed to anyone. `/?feed=1`
is a parameter on the root rather than a `/feed` path because a new path would
need a route.

**The syntax sheet is markup, held to the parser by a test.** It is prose about
a parser, not behaviour, so it lives at the end of the body (a modal is painted
in the top layer; inside `.hero-inner` it would be one more child of that
layout). `help.test.mjs` fails on a prefix query.js lifts that the sheet does not
name, and on a token named here that query.js would leave as words. A help page
that has drifted from the parser reads like an answer and is worse than none.

**The people grid steps through divisors of the cell caps.** `repeat(3, …)` put
about 250px between two 64px faces on a laptop; `auto-fill` with a minimum cell
left the `+N more` count alone on a second row at every width where the columns
did not divide the cap, five of them around 700px. 6 and 3 divide both caps
(base.js: 6 and 24), so every row is full and the count always closes one.

**The observer list is capped in width.** One 60-character display name sized
the list at 438px and gave the whole page a horizontal scrollbar on a phone.

**Avatar badges scale with the face, their digits less.** One 15px badge for
every size was right on a 34px result face and nearly the whole of a 22px byline
one; strictly proportional digits are 6px at 22px, a smudge rather than a number.

**Article covers are 16:10, not square.** Square-cropped they lost a third of
every image from each side, and centred they left the title floating beside the
middle of the picture. `height: auto` is required because an explicit height
beats `aspect-ratio` and `.thumb`'s square one is inherited.

**The unlock row is a row, not a button.** As a button outside the arrow walk
and the tab order it left a keyboard-only reader no way to reopen a dialog they
had dismissed.

## observer_stats.html

**Concurrency is per relay, not only across relays.** One relay can hold most
of the (observer, service) pairs, so parallelising across relays alone did not
shorten a run; `PER_RELAY_CONCURRENCY` is what did.
