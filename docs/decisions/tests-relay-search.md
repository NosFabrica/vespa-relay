# Relay search test decisions

What the relay's search and reference-expansion tests used to carry in their
comments and no other decisions file records. The expansion's move into the
store, the read lens and the per-connection search gate are in
`relay-server.md`.

**Several pointers naming one subject is the common page, not a corner.** In a
production sample of NIP-32 labels, 76 targets were named by more than one
label and ten of them by ten labels each, so a page of labels on a busy topic
converges on the same handful of notes. Without the sent-set that is up to ten
copies of one note on one subscription; `many pointers naming one subject send
it once` pins it, and every `page()` in the suite rejects a duplicate.

**Members are spliced in tag order because that order is the rank.** Every one
of the eleven real Trusted Lists on staging orders its members by the score the
publisher computed, all 180 members scored and every list sorted descending.
The relay reads the key and never the score, so tag order is the only thing
that makes a spliced member's position mean anything; store order would mean
whatever the mirror had caught up on. The per-event cap truncates the lookup,
not the result, for the same reason: a truncated splice is the top of the
publisher's own ranking.

**A kind-restricted search costs its recall plus the pointer companions.**
Since the store's conversion recall (vespa-eventstore#88) a search restricted
to one kind also fetches the pointer kinds that could name it: labels for
everyone, the event- and addressable-shaped declaration families from the
reader's enrolled signers only. `a search for the external-id family spends the
companion recall and nothing more` pins that shape, not a count: every query
carries the terms, the declaration companions name nobody the reader did not
enrol, and no lookup follows the page. The 10040 pass is paid on the write
path, not per REQ.

**The doubling-store case moved with the expansion.** That the expansion
refuses to emit a row twice even when the store hands the same row back twice
is stated in vespa-eventstore's `SearchExpansionTest` against a doubling index.
`a row the store hands back twice still goes out once` is left in the relay as
a seed-only body because no wrapper remains between the relay and its store to
state it from here.
