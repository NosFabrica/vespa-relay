# Web test suite decisions (batch B)

The history behind the module tests under `web/src/test/js/` (all but the
sync, cards, query, provenance and verdicts suites) and the browser probes
under `web/src/test/browser/`, moved out of the sources so the tests read on
their own. One paragraph per decision; `git log -L` finds the commit.

**An absence is cached only when the relay finished answering.** Caching
"no profile here" off a timed-out read records a fact the relay never stated,
and because the cache is consulted before every render the absence is
permanent for the session. The mistake was made three times: in profiles.js
(a dropped lookup poisoned the reader's own key, so signing in appeared to
need a refresh), in app.js's score chips, and in the reply-parent lookup,
where a cached null renders "in reply to note1qqq…" on every reply to that
parent. profiles, parents, groupnames and providers each pin it.

**The first ask is the page and its preload in one REQ.** The earlier design
asked one page first and three more behind it, on the belief that a shorter
ask answers sooner. Measured on staging on 2026-09-03 it did not: a ranked
search costs its match set, and `limit: 1` took as long as `limit: 200`, so
two asks for one answer were two full searches slowing each other on one
engine. `firstAsk(0) === askLimit(0)` in paging.test.mjs holds it.

**The mirror scope fixture must have the shape `/stats.json` serves.** Every
section is wrapped in the `{status, generatedAt, tookMs, data}` envelope.
The fixture in mirrors.test.mjs once omitted it, so `mirrorScope` read
`stats.sync.mirrors`, passed every case, and returned null against every
real document: the readiness panel failed closed and stopped asking, with no
wrong number to notice. The bug it hid was "35% mirroring" (31,118 here of
89,485 upstream) on a mirror missing nothing, because only our count was
narrowed to the mirrored kinds.

**The observer token is hex, and the sheet must say so.** Measured against
`wss://search-staging.brainstorm.world` with a real 10040 observer:
`observer:<64 hex>` reorders the answer, and `observer:<npub>` for the same
key returns byte-identical results to sending no observer at all. The store
ignores the bech32 form rather than refusing it, and the page shows the npub
everywhere (`?as=npub1…` is in its own URL), so an example spelt that way
would teach the one mistake the sheet exists to prevent.

**A bare `30392` tag in a 10040 is a delegation.** The Map measured on
staging on 2026-09-01 carried `30382:rank` and `30382:followers` naming one
service and a bare `30392` naming another. The bare form has no `:`, so
quartz's ServiceProviderTag never parsed it, and a reader taking only the
NIP-85 shape resolved list delegations to the empty set and drew no list
pill, silently. providers.test.mjs pins both shapes; ObserverTrustListIT
covers the serving side.

**The pointer gate moved to the client as `authors`, in two asks.** The
anonymous reference socket narrows nothing: a bare `kinds:[30392]` by member
answered with every publisher's list (seven lists naming two probed members
on staging, only six from the delegated publisher). So every declaration
filter carries `authors` and an undelegated kind is not asked for. The gated
and open halves used to share one REQ and one EOSE, so the small
author-narrowed read waited on an open label read six times its size that
drew nothing: 252ms against 67ms over a page of 42.

**The picker keeps every row the relay returned.** rank() once re-tested the
relay's 39000 answers with `includes` on name and id. The relay matches
`name` in the primary tier and `about` in the secondary, both through the
fuzzy `near` column, so the re-test discarded every hit the index could make
and a substring could not, reporting "No group matches". The fixtures could
not see it because "chachi" is a literal substring of "Chachi Fans".

**The reader's own 10009 is read on the authenticated socket.** The store
applies the observer as a filter, so a reader with no scores mirrored here
reads back nothing, their own events included: `{kinds:[10009],authors:[me]}`
returned 0 against a store with no scores and 1 the moment a trusted provider
scored them. Moving the read to the anonymous socket would make it answer,
which is why it must not: the picker would be the one place showing a reader
content the relay has decided it cannot rank for them.

**A NUL byte in a source file makes git call it binary.** searchfield.js
carried a literal NUL inside a template string for four commits; git sniffs
the first 8000 bytes, so every diff read `Bin 23897 -> 23932 bytes` and grep
answered "binary file matches". Nobody fixed it: the header comments above
the byte grew past 8000 and diffs came back on their own, so any edit that
shortened them would have dragged the module back over the line. The fix is
the escape `\u0000`; source.test.mjs rejects the byte at any offset.

**The preload crawler must match side-effect imports.** Matching only
`from "…"` missed `import "./x.js"`, which is how cards.js pulls in the kind
registry, so the family modules were invisible to the crawler, unhinted in
index.html, and reported as matching "exactly" while sitting on the slow
path.

**A `/` shortcut ate letters out of the search box.** The box is a
contenteditable div, invisible to a tag-name test for "is somebody typing",
so the shortcut fired mid-query. `isTyping` in keynav.js is one function with
one contract, and keynav.test.mjs holds the contenteditable case.

**Absolute asset paths get 200s from the wrong service.** The status pages
are mounted under path prefixes with a strip rewrite; an absolute
`/web/shared/page.js` is asked of the host root, where the relay serves its
own copy of every file name, so the page renders with another service's
modules and nothing 404s. A bare `web/shared/page.js` specifier is worse: it
is an import map key, not a relative url, and fails the whole module graph
without a request.

**A green row probe does not cover the reader's own NIP-51 lists.** The
probe passes the observer as `as=` rather than signing in. Since store
`2bc79f5f40` a people list (30000) or follow pack (39089) splices its
members, but provenance.js draws that pill only for the reader who signed
the list, and the lens says whom to rank through, not who signed.
Exercising it needs a corpus holding one of the observer's own lists naming
somebody whose profile is on the page; `fetch-observer-corpus.mjs` will hand
back a corpus with no such list and the case is silently untested.

**`?page=99` sat on a skeleton forever.** settlePage() corrected the state
behind a page that did not exist and nothing repainted; the pager probe holds
that a url past the end lands on the last page there is. The probe also
holds that a page turn writes the url from `hitsFor`, not the box: typing a
new search without submitting used to write `?q=<typed>&page=2` over the
results on screen, and a link to it came back empty.
