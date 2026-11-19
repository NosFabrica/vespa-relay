# keywatch: leaked, moved and vouched-for keys

`:keywatch` is a standalone process that reads one relay (by default the production
`wss://search.brainstorm.world/`, **read-only**) and writes a JSON report of keys whose secret is
public, keys that say they moved, and whether anyone trustworthy backed the move. It depends on
quartz alone: no store, no Vespa, and no module of ours, so it can leave this repo unchanged.

```bash
./gradlew :keywatch:test
./gradlew :keywatch:installDist
KEYWATCH_OUT=/tmp/keywatch-report.json keywatch/build/install/vespa-keywatch/bin/vespa-keywatch
```

A full pass over production reads about 200k events and takes over an hour. Run it from an
installed copy, not `./gradlew :keywatch:run`: `run` loads classes straight out of `build/`, so a
compile while it runs swaps them underneath it and the pass dies on a `NoClassDefFoundError` an
hour in (measured: the first production run). Copy the `install/` directory elsewhere if you will
keep building.

| Variable | Default | |
|---|---|---|
| `KEYWATCH_RELAY` | `wss://search.brainstorm.world/` | the relay to read |
| `KEYWATCH_READ_TOKEN` | `include:spam` | appended to every filter's `search`; the relay answers an undeclared read with `CLOSED auth-required:`, and spam-flagged events are where the bot farms are |
| `KEYWATCH_OBSERVER` | `460c25e6…065c` | whose kind 10040 names the `30382:rank` provider that ranks vouchers |
| `KEYWATCH_OUT` | `keywatch-report.json` | written atomically |
| `KEYWATCH_INTERVAL_MINUTES` | `0` | `0` runs once; otherwise the process repeats |
| `KEYWATCH_MIN_VOUCHER_RANK` | `50` | |
| `KEYWATCH_MIN_VOUCHERS` | `2` | |
| `KEYWATCH_VOUCHER_ABOVE_NEW_KEY` | `true` | a voucher must outrank the key it admits |
| `KEYWATCH_REQUIRE_KNEW_OLD_KEY` | `false` | only vouchers following the old key count |
| `KEYWATCH_MAX_DEGRADED_WAIT_MINUTES` | `60` | how long to wait for a partial index to heal before giving up the pass |

## Classes

A key can hold several. `status` says how much the evidence carries:
`PROVEN` (arithmetic or a signed NIP-62), `VALIDATED` (third parties vouched), `CLAIMED` (only the
keys involved say so), `CONTESTED`.

| Class | Evidence | `since` |
|---|---|---|
| `COMPROMISED_PUBLIC` | a published nsec, hex secret or NIP-06 mnemonic derives this key, and the key had a life before: a rank card, or an event signed at least a day before the leak | the first event that published it |
| `PUBLIC_FROM_BIRTH` | the same, but the key had no such life (the bot farms sign one profile a median 83 s before naming themselves after their nsec), or the secret was handed out in an invite link (`iris.to/chat#nsec1…`) | idem |
| `COMPROMISED_ACCESS` | a transition whose wording claims a compromise | the first claim |
| `SUSPECTED` | a precautionary move ("may have leaked, just in case"), or a posted `ncryptsec` | idem |
| `LOST` | a transition claiming a lost key | idem |
| `RETIRED` | a transition with no compromise claimed | idem |
| `SECONDARY` | "alt / bot run by / backup account of npub…"; `VALIDATED` when the primary names it back | the claim |
| `VANISHED` | a NIP-62 request | the request |

`activeAfter` on a `COMPROMISED_PUBLIC` key is its newest event after the leak: the owner, or
whoever else now holds the key. Those keys sort first in the report.

## The vouching rule

Neither key's word admits a transition. The old key may be in an attacker's hands, and anyone can
mint a key that claims an old one. A transition is `VALIDATED` only when
`KEYWATCH_MIN_VOUCHERS` third parties endorse it, each of them:

- not one of the two keys, and not a key that is itself `COMPROMISED_PUBLIC` or `PUBLIC_FROM_BIRTH`;
- citing the claim after it was made: a repost (6/16), a quote (`q` tag or inline `nevent`), or a
  note of their own naming both keys;
- ranked at `KEYWATCH_MIN_VOUCHER_RANK` or more by the observer's provider, and above the new key.

A reply never endorses. A quote, reply or statement with warning words (`scam`, `fake`,
`impersonat…`, `don't follow`) from an eligible voucher counts against, and one against makes the
transition `CONTESTED`. An old key with several claimed successors is `CONTESTED` for all of them,
unless exactly one is `VALIDATED`. `successor` is set only on a `VALIDATED` transition;
`relatedKey` always names the claimed one. Every citation is in `transitions[].vouches`, with the
reason it did or did not count.

Ranks are per observer, so "validated" is too: the same evidence can validate under one lens and
not another.

## How it reads the relay

- **Secrets**: NIP-50 searches (`nsec1`, `ncryptsec1`, `privkey`, `"private key"`, `"secret key"`,
  `"seed phrase"`, `mnemonic`). A 64-hex string counts only right after a word naming a secret; a
  mnemonic only as a checksum-valid run of BIP-39 words.
- **Statements**: phrase searches over kinds 0 and 1, each paired with `npub`/`nprofile`. The
  phrase must lead into the key, ending at most 30 characters before it ("my new npub is …",
  "follow me at …", "I have moved to …", "my old account: …"), with nothing recommending someone else
  in between, and a note needs a first-person word before it. In a note "follow us on …" is a footer advertising an account (a daily bulletin carried
  one in each of ~100 issues), so only a profile may say it; a note's reason is read from the 300
  characters before the key. "Created a new account …" is a
  secondary; an "old account" phrase that redirects ("Old account. New one at …") makes the speaker
  the old key. Exactly one foreign key makes a self claim; two make a third-party statement; more
  are dropped. Measured on the first report's 1,809 claims: the looser reader it replaced had half
  of its ten validated transitions wrong (a birthday post, a recommendation, "my signal
  conversations have moved to …"), reposted for what they were, not for a key change; this one
  keeps 866 and keeps none of those. `StatementReaderProbe` reruns that check on any JSONL of events.
- **Paging**: search answers are ranked, so an `until` cursor does not walk them (the second page of
  `nsec1` returned 44 of 26,820). `Sweeper` sizes each yearly window with COUNT, halves it until it
  fits under 4,000, then fetches it whole.

## A partial index

While part of its Vespa index is unreachable (`non-ideal-state`: a cluster settling), the relay
refuses every COUNT (`CLOSED error: vespa searched only 80% of the corpus…`) but answers a REQ
from what it can reach, with a plain EOSE and a `partial:` NOTICE at most once per ten minutes per
connection. Measured 2026-11-18: the observer's kind 10040 came back as an empty EOSE. keywatch
decides several things from absence (no event before a leak makes a key `PUBLIC_FROM_BIRTH`; no
mention back leaves a secondary `CLAIMED`), so a partial read would misclassify silently.

So every phase starts only once a canary COUNT is answered, and is redone from the start when one
of its reads was partial: a `partial:` NOTICE, a COUNT refused for coverage, or a canary refused
every 25 fetches and when the phase ends. After `KEYWATCH_MAX_DEGRADED_WAIT_MINUTES` of waiting, or
five partial attempts at one phase, the pass ends and writes no report; the previous report stays.
`DegradedRelayTest` pins it: without the guard, the victim it hides comes out `PUBLIC_FROM_BIRTH`.

## What the report never holds

No secret: `Exposure` keeps only the derived public key, and `KeyWatchTest` fails if an `nsec` or a
secret's hex reaches the JSON. The `evidence` ids do point at events that still carry the secret,
so a report naming keys that are still active is sensitive: don't publish it as is.
