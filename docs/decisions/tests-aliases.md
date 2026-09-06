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

**The shared-name fold needs every url to have answered, and counts a
credential refusal as an answer.** `foldUnreadableGroups` collapses a host
none of whose urls served a window through any filter onto its preferred url,
on the hostname alone; it is the one fold that rests on no measurement. It
fires only when every url in the group answered (an EOSE or a CLOSED, never a
null page), or our own outage would become a signed claim about their server,
and only when nothing on the host served anything at all, since a window from
any member is a measurement and beats the default. The rule does not ask what
kind of answer came back: a url that refused our credentials and one that
served an empty EOSE fold together. `AliasFoldingTest` pins that as a limit
rather than a virtue; tightening the rule to demand the same kind of answer
inverts that test.
