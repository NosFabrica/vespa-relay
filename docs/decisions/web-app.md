# Search page decisions

The history behind `web/src/main/resources/web/app.js`, moved out of the
source so the code reads on its own. One paragraph per decision; `git log -L`
on the function finds the commit.

**The type-ahead runs one ask at a time, and the queued text goes through the
debounce again.** A ranked search is the one read the relay cannot make cheap,
and the engine shares its match threads between concurrent asks. Measured
against staging (2026-09-03), the popup's asks for `b`, `bi`, `bit` up to
`bitcoin` were seven ranked searches in flight together, answering in 1.6s to
7.4s each, and the Enter press at 2.4s came back at 9.1s for a query that
answers alone in 3.7s. Running the queued text at once searched `bitco` for
four seconds while the reader had finished typing `bitcoin`, so it re-arms the
debounce instead. The 250ms debounce itself replaced 150ms, which fired on
nearly every keystroke of ordinary typing.

**Sign-in is a shared in-flight promise, not a flag.** With a bare `loginTried`
flag, the type-ahead fired searches while the first caller sat in the
extension popup; each saw the flag set with `me` still null and sent its REQ
unauthenticated. A relay does not re-run a subscription it answered under the
old auth state, so those results stayed unranked with nothing on screen to
say so.

**There is no "as me" toggle beside the observer picker.** Authenticate-off
plus observer-picked was a legal combination that did nothing, because the
lens needs an authenticated reader. One thing you choose (whose trust) and one
thing that just happens (being signed in) is the same feature without the
inert corner.

**Tab kinds follow the families in shared/kinds.js.** The Media chip once asked
for 31922, a NIP-52 calendar date that renders under Live, and left out 1986
audio, so the audio kind was unreachable from any chip and every Media result
set could contain a conference date.

**The remembered face and the sign-in retries key on `has`, not `get`.** The
profile cache records `null` for a pubkey the relay answered about and has no
kind 0 for. Retrying on that spent 3.6s of sleeps and three more REQs
re-asking an answered question for every account with no profile; only a read
that came back with nothing at all is repeated, and a dropped read is never
cached as "no picture".

**The observer list is read anonymously and in parallel chunks.** The
authenticated socket is gated to authors the reader has scored, so asking there
listed only observers the reader already trusted, and a picker that hides
everyone you have not met cannot introduce you to anyone. The list is small
(271 observers against 12.28M profiles), and its two chunks used to go out
serially, so the first open cost two full waits.

**The reader's own kind 10009 stays on the authenticated socket even though it
comes back empty for an ungated reader.** The store applies the observer as a
filter, so a reader whose trust chain has not reached this relay reads back
nothing, including their own events; against a real store the read returned 0
of the reader's own event and returned it the moment a trusted provider scored
them. Routing this one read down the anonymous connection would make the
group picker the single place showing content the relay has otherwise decided
it cannot rank for them. Whether an observer should be gated by their own
trust at all (the tensor has no self-edge, so you score 0 under your own lens)
is the store's question, and nothing here anticipates the answer.

**The score chips and the provenance row make the same kind 30382 read, and
the duplicate is left.** Over a page of 42 profiles the two came back with the
same 42 events, 20 KB each way, on every search. Joining paintScores onto the
read already in flight made it worse (4 asks instead of 2): paintScores has no
in-flight dedupe, its `need` set is computed after every await, and its
callers are plural, so a network await before `need` turns a one-tick window
into a round trip in which two calls both ask. Make it coalesce concurrent runs
first, then join; one 39ms round trip is not worth a coalescing bug in the
code that fills every avatar's number.

**Scores are written by service priority, not arrival.** `authors` is an OR,
so a reader naming two rank services gets both services' cards interleaved,
and writing each as it came made the number on a face depend on which card the
relay sent last. A `null` is cached only after EOSE: `req()` also resolves on
its timeout, and a null recorded off a slow read was permanent for the lens.

**The provenance pointers are two asks, and a repaint keys on what is drawn,
not on a count.** Sent as one REQ the gated, author-narrowed half shared one
EOSE with the open half, which is six times its bytes and cannot be narrowed;
the pills a reader asked for waited on the ones nobody did, 252ms against 67ms
over a page of 42 profiles. A count stopped being a change signal once the
second seed could remove a pill as well as add one and net to zero.

**The export carries no union caveat.** It used to warn that a multi-filter
REQ came back as each filter's ranked run end to end, so a jump back up the
trust scale was a seam. Since vespaEventStore 8a45e4d1a2 the store merges the
filters of one REQ on the engine's scores when they share a rank profile,
which this page's hashtag filters always do, so the order is one ranking of
the union and a jump back up the scale is worth challenging.

**The popup and the results view have separate state.** One object used to
serve both: a keystroke over a page of results replaced `hits` with the
popup's eight rows, invisibly, until the json toggle answered "no longer in
the current results" about a card on screen. With a pager reading the array
constantly, Next would have cut page two out of a query nobody submitted.

**The page url is named from `hitsFor`, never from the search box.** Typing a
new query without submitting it and turning the page wrote
`?q=something+else&page=2` over page two of the search still on screen; shared,
that link came back with no results. `settlePage()` reports whether it moved
because `?q=…&page=99` once corrected the state and drew nothing: the
skeleton for the page that was asked for sat there forever.

**A widening ask repaints the pager alone when the cards are unchanged.** An
innerHTML rewrite of forty cards is 1 to 2ms, but it destroys every card
element: the lazy-media observers re-arm, loads in progress are dropped, and
any selection the reader was making goes with them.

**The pager's known waste is left alone.** Counted on the wire over a
305-event corpus, signed out, landing on page one is 4 REQs (search, preload,
two pointer reads) and three page turns cost 6 more. Each widened ask re-sends
the pages already held, which is the missing NIP-01 offset paid behind the
reader. hydrate() re-asks the pointers of the whole buffer because the
provenance row seeds by replacing; narrowing that means caching pointers
across asks, which changes what the row means. Sampled at every repaint over a
corpus with 120 NIP-32 labels the pill count went 0, 35, 35, 35 and never
dropped, so the re-seed window is real but no flicker was reproduced.

**The feed is `/?feed=1`, not `/feed`.** The root already serves the page from
cold, and a new path would need a server route before a reload of it could
work. The feed preview under the hero draws only for a signed-in reader:
signed out the same read is the firehose, which the hero does not stand behind.

**Signing out runs one search, not two.** `setViewingAs(null)` inside the
sign-out click re-ran the search, and the click's own `finally` ran it again,
so every sign-out sent two REQs and threw the first answer away.
`applyViewingAs` is the render-only half for exactly this caller.

**A popup pick navigates through `selfHref`.** The path was once built here as
well (`kind 0 ? npub : note`), the same rule the cards apply written twice, and
the copy here had no guard: an event with no id navigated to "/" and looked
like the picker had reset the page.

**Video urls wait in `data-src`.** `preload="metadata"` is a range request per
card, two for an mp4 whose moov atom is at the end, so a search returning
sixty short videos opened sixty of them before the reader had scrolled past
the second.
