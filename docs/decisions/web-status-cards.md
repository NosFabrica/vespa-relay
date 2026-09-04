# Status card decisions

The history behind `web/src/main/resources/web/sync/cards.js` and
`monitor/cards.js`, moved out of the source so the code reads on its own. One
paragraph per decision; `git log -L` on the function finds the commit.
`web-shared-sync.md` covers the judgements these cards draw.

## sync/cards.js

**The monitor's passes are their own card, on their own page.** The alias
fold, the stability gate and fitness sat on the sync card because they arrived
in the same processor array, and the card asked two questions at once: "is
the mirror keeping up" is answered by streams, queue depth and ingest, while
"which relays may we dial at all" is answered by a round-up and three passes
whose unit is a url and whose output is a signed kind-30166 record. An
operator arrives with one of those questions. Each plane keeps its own
`Processors` now, so nothing sorts rows by name at render time.

**A mark that reads the same on every healthy system is not a mark.** `100 of
512 workers downloading` was both config ceilings (`concurrency` and
`admissionWidth`), so every busy stream on every deployment rendered exactly
that. It was the implementer's argument for two gates, printed where an
operator scrolls past it; that argument lives in the KDoc.

**The coverage strip replaced a per-relay table, and the prime-relays panel
is not that table back.** The table was 10,462 band rows behind a filter box,
and "is the mirror deep or only recent" needed all of them read; a
distribution answers it in one glance. The relay panel asks the opposite
question, which relays are synced at all, and a distribution cannot answer
it: a relay never reached or refused on every visit has no band and appears
in no distribution. Its subject is the roster, so its denominator is honest
where the strip's cannot be.

**The mirror-wide cut is two lines, not four tables.** The card drew the
mirror's four pool tables and then the same rows again cut by stream, so
every held relay appeared twice, and with one stream both cuts were the same
tables under two headings. What the mirror-wide cut answers that a stream's
cannot is proportion, how much of the router is re-fetching rather than
keeping up, and that is a tally. It is withheld below two streams, so the
truncation disclosure moved to card level: a disclosure that only appeared on
multi-stream deployments was not one.

**The status line carries both ends of the ingest queue.** With the drain
alone, staging with the queue pinned full read `0 events/s`, which is what a
store that stopped answering looks like and also what a fan-out gone quiet
looks like, since the drain counts what left a batch. What is still arriving
is the reading that tells them apart.

**The pool heading carries the count, not the denominator.** `12 of 412` on
all five headings, three lines under a roster line that had just said 412,
was the same number five times in one section.

**One clock per pool table.** `Date.now()` per row let two rows in one table
be measured against different instants and draw the same cursor two ways.

## monitor/cards.js

**The verdicts panel reads records, not the rollup.** `/stats.json` says 373
urls collapsed onto 909 relays and stops; it cannot say what the store holds
about one url or when it was measured. The kind histogram lost the
protocol-check property when it stopped asking NIP-45 COUNTs and was right to,
because counts have a rollup to fall back on; verdicts have no other reader.

**The read is scoped to the relay's own key.** A router discovers relays from
other monitors' reports (the outbox source reads `kinds: [10002, 10050, 30002,
30166]`), so a mirror holds strangers' 30166s by design and public monitors
publish them by the tens of thousands. Unscoped, "1,621 folded away" silently
included relays this router never probed. A COUNT of the whole kind reports
what the scope left out, so scoping hides nothing. `self` is the NIP-11
`self` field, not `pubkey`, which is the admin contact.

**The verdict's age is the tag's, not the record's.** Kind 30166 is
addressable and shared, and quartz's passive monitor rewrites it on every
connection, so `created_at` tracks the last time we talked to the relay, not
the last time we measured it. Reading the record's clock made half the
verdicts look immortal on the Kotlin side; both dates are on screen under
their own names.

**The two verdicts are drawn separately.** Collapsed, a url the stability pass
had measured but the fold had not was drawn `no verdict` with the stability
sentence under it, so a host mid-measurement read as one nothing had looked
at, beside evidence contradicting the word above it.

**A cleared verdict expires too.** The currency test was on the fold row only,
so a retired cleared verdict was still drawn `keep` while the url was back in
the queue. Half the verdicts in a store are the cleared form, so after a rules
bump most of the page said the opposite of the truth.

**`no verdict yet` is not `not folded`.** The tile was spelled `not folded`,
the same words the fold's pill wears on every row it did not fold, and the
grade pulled the two counts apart: 77 urls carried no verdict of any kind
while 540 rows read `not folded`, 510 of them graded minutes earlier.

**`ago` takes the instant.** Handed a duration it drew every verdict as
`20679d ago`, which is `now` minus five minutes read as an epoch.

**The grade is a pill, not the software column.** The monitor wrote the
fitness grade on `s`, so a dead relay showed `dead` in the slot where every
other NIP-66 reader shows strfry's repository url, and the most consequential
of the three verdicts could not be drawn as one.

**The Tor tile is drawn at zero.** The other conditional tiles say "this state
does not occur here"; this one says "nothing we admit is behind a hidden
service", which is the answer an operator running Tor came for.
