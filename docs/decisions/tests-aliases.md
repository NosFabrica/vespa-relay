# Alias test decisions

What the alias fold tests used to carry in their comments and no other
decisions file records. The fold's own history, its measured hosts and the
verdict record's rules are in `aliases.md` and `peers.md`.

**The pass counts the urls that arrived undecided, not the candidate set.**
`Processors.Work.newUrls` is what the card draws its position against. Counted
against `candidates`, a settled host of two urls read `2 of 2 checked` on a
pass that checked nothing, and a pass that abandoned a group could show more
urls left undecided than had arrived so.

**A leader too thin to be a yardstick keeps its members off the wire.** A
leader handing over fewer than `minSample` ids can neither be folded onto nor,
since the thin-window guard, cleared against, so dialling the members behind
it decides nothing; it did so again on every pass. The leader itself still
costs one probe per pass, which is how its recovery is noticed, and its
`ws://` twin is still decided, because that pairing rests on both answering
rather than on a window.

**The in-flight release in the mid-pass test is unconditional.** The test
parks the second group on a `CompletableDeferred` while it reads the first
group's verdict. Completing it only on the happy path left a failing run with
the pass parked forever and `runBlocking` waiting on it, so the assertion error
never surfaced and the suite hung instead of failing.
