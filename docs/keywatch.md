# keywatch: leaked, moved and vouched-for keys

`:keywatch` is a standalone process that reads one relay (by default the production
`wss://search.brainstorm.world/`, **read-only**) and writes a JSON report of keys whose secret is
public, keys that say they moved, and whether anyone trustworthy backed the move. It depends on
quartz alone: no store, no Vespa, and no module of ours, so it can leave this repo unchanged.

```bash
./gradlew :keywatch:test
KEYWATCH_OUT=/tmp/keywatch-report.json ./gradlew :keywatch:run
```

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

## Classes

A key can hold several. `status` says how much the evidence carries:
`PROVEN` (arithmetic or a signed NIP-62), `VALIDATED` (third parties vouched), `CLAIMED` (only the
keys involved say so), `CONTESTED`.

| Class | Evidence | `since` |
|---|---|---|
| `COMPROMISED_PUBLIC` | a published nsec, hex secret or NIP-06 mnemonic derives this key, and the key had a life before (an event, or a rank card) | the first event that published it |
| `PUBLIC_FROM_BIRTH` | the same, but the key signed nothing before, or the secret was handed out in an invite link (`iris.to/chat#nsec1…`) | idem |
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
- **Statements**: phrase searches over kinds 0 and 1, each paired with `npub`/`nprofile`. A note's
  claim needs first-person wording near the key. Exactly one foreign key makes a self claim; two
  make a third-party statement; more are dropped.
- **Paging**: search answers are ranked, so an `until` cursor does not walk them (the second page of
  `nsec1` returned 44 of 26,820). `Sweeper` sizes each yearly window with COUNT, halves it until it
  fits under 4,000, then fetches it whole.

## What the report never holds

No secret: `Exposure` keeps only the derived public key, and `KeyWatchTest` fails if an `nsec` or a
secret's hex reaches the JSON. The `evidence` ids do point at events that still carry the secret,
so a report naming keys that are still active is sensitive: don't publish it as is.
