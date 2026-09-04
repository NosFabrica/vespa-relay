# Ingest test decisions

What the ingest, refused-id and stats-snapshot tests used to carry in their
comments and no other decisions file records. The dedup, supersession, wedge,
cuckoo-geometry and window-lookup history is in `ingest.md`; serving pressure
and the writer topology in `monitor-engine.md`; the stats tiers in
`relay-maintenance.md`.

**Empty kind-0 content is not a parse failure.** "Expected start of the
object '{', but had 'EOF' instead" was the second-largest class in a live
parse audit, 3,783 of 77,753 reports. quartz cdef4e9658 reads an empty
content as an empty profile, which is what it always meant; the test pins
that it no longer produces a finding.

**Cuckoo and refused-id tests use real SHA-256 ids.** The filter slices its
bucket out of the id's first 16 hex characters and its fingerprint out of the
next 8, with no hashing of its own. A counter formatted as `"%064x"` is all
zeros across both slices, so every such id lands in one bucket with one
fingerprint and they are all hits on each other, which reads as a broken
filter and is a property of the test data.

**A staleness notice is cleared only by the tier that left it.** Charts had
been failing for hours beside counters forty seconds old, publishing every
minute, and every one of those publishes cleared the notice that said so.
The page then drew a document indistinguishable from a healthy one.
