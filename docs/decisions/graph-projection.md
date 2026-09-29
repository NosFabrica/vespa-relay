# Graph projection decisions

The history behind `common/.../graph/` and `relay/.../server/GraphRoutes.kt`, the wiring of
[neo4j-eventstore](https://github.com/vitorpamplona/neo4j-eventstore) (its `docs/spec.md` is
the design). One paragraph per decision.

**A projection, not a second store.** Neo4j runs no Nostr policy. Vespa decides what is stored
(dedup, the NIP-01 tiebreak, NIP-09, NIP-62, expiry, bans), and Neo4j follows those decisions.
Running the policy twice would only create a second opinion that can disagree.

**Changes come from the store's index seam, not from this relay's write sites.** This relay has
at least seven write call sites. None of them sees the removals the store makes internally:
supersession, kind-5 targets, vanish, expiry, orphan-score sweeps. Every one of those passes
through the store's `EventIndex`, so the store gained `open(observers = …)`
(vespa-eventstore `bebbf90493`). Both processes pass the projection's listener there. An
`IEventStore` wrapper was rejected because it sees a kind 5 arrive but not what it erased, and
it would break the `as? VespaEventStore` casts here.

**The observer is registered before the store exists; the source is bound after.** The store
takes its observers at `open()`, and the projection's reconciler reads the store. `VespaSource`
is bound right after the store opens. Nothing reads it before the reconcile loop starts, which
happens later in `RelayMain`.

**Only the relay process reconciles.** Both processes feed. One reconciler is enough, since it
treats Vespa's id set as the truth, and two would repeat each other's windows. The relay is the
process that already runs the maintenance jobs.

**The Cypher route chooses its status before streaming.** `precheck` runs the guard alone, so a
refused query is a 400 and never a 200 cut short.

**The NIP-98 token is verified with the body.** A token signed for one query cannot carry
another.

**No query limits in v1, by decision.** Limits will be set from production measurements. Until
then `GRAPH_CYPHER` should stay at `admin`.
