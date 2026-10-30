# Module boundary decisions

The history behind `settings.gradle.kts`, the six `build.gradle.kts` files and
the package map, moved out of the source so the code reads on its own. One
paragraph per decision; `git log -L` on the function finds the commit.

**The include order in `settings.gradle.kts` is the layering.** The modules are
included `common, web, peers, monitor, relay, sync`, and a dependency may only
point backwards along it. That one rule carries every edge the prose used to
carry separately: `:common` can depend on nothing, `:monitor` cannot reach
`:sync`, and a new module declares where it sits by where it is included.
`ModuleBoundariesTest` reads the order out of the file rather than repeating it,
so the rule cannot drift from the list it is about.

**A package belongs to exactly one module.** Six packages used to span two or
three: `config` was `:common`, `:peers` and `:relay` at once, `maintenance` and
`server` were `:common` and `:relay`, `progress` was `:peers` and `:sync`. The
cost is not correctness — Kotlin is happy — it is that a package name stopped
answering "which module holds this file", which is the question you ask before
every other one. `:common`'s share became `identity`, `pressure` and `store`,
`:relay`'s became `server.config`, and `:sync`'s two progress files joined the
`status` package that already held the reports they feed. `:peers` kept `config`
because `sync.conf` and `monitor.conf` are the configs this repo means.

**The root package holds one entrypoint per process and nothing else.**
`SyncEngine.kt` sat beside `RelayMain.kt` in `com.nosfabrica.vespa.relay`, which
made the root package the only one whose contents were an accident of history.
It moved to `sync/`, next to the mirror it wires. The rule that survives is
narrow enough to test: a file in the root package is a `*Main.kt`.

**A process module exports nothing.** `:relay` and `:sync` declared every
dependency as `api`. Nothing compiles against a process, so `api` there says
only that the author did not choose; `implementation` says the dependency stops
here. The two modules that are compiled against — `:common` and `:peers` — keep
`api` for the libraries in their signatures, with a line saying which signature.

**A dependency nothing imports is a false edge.** `:monitor` declared
`api(project(":web"))`, and the only file that used it was one test serving the
shared page over the monitor's document. The declaration put Ktor and Netty on
`:monitor`'s compile classpath and re-exported them to `:sync`, in a module
whose whole contract is that it produces a document and renders nothing. It is
now `testImplementation`. `:monitor` also used okhttp in `RelayDocument` without
declaring it, riding on `:peers`' `api(libs.okhttp)`; declared, the graph says
where the dependency is used. `ModuleBoundariesTest` checks both directions:
every project dependency in a compile scope is imported by that module's main
sources, and every test-scope one by its tests.

**The guards that read the checkout live in `:common`, and declare it as an
input.** `NoBrowserFilesInEngineModulesTest` and `ProbeSwitchesAreForwardedTest`
scan every module but lived in `:sync`, so the mirror policed the relay and
`./gradlew :relay:test` ran neither. They moved to `:common/…/arch/` with the
new boundary guard. All three read files rather than call code, which is the
trap they were one of: Gradle knew none of those files as inputs, so the task
went `UP-TO-DATE` after exactly the change the guards exist to catch. It was
checked by breaking each of the ten rules and watching the ten cases fail; on
the first attempt the whole task was skipped and the build was green. The inputs
are declared now, the same way `:relay:test` declares `.env.example`.

**What is not ours goes upstream, and the engine import list only shrinks.**
A cross-repo review found code here re-deriving what quartz or the store
already does or should: a Hex check, NIP-01 prefix parsing, a default-port
fold quartz's normalizer should own, the store's search grammar, YQL against
schema field names. Each copy drifts on the next bump without a test noticing,
and two already disagreed with their upstream. So the rule is to search
upstream first, and a copy that has to stay names the upstream change that
retires it. The store's `engine` package is its internal layer, reached through
its escape hatches "for ops and benchmarks"; seven main files imported from it,
and `ModuleBoundariesTest.ENGINE_ALLOWED` lists exactly those names, failing on
a new one and on a stale entry, so the list can only shrink. The store's facade
release retired two (`StoreProbes`, `SchemaDeploy`) and admitted one,
`StatsVespa`'s `EventQuery`, because `EngineReads.group` takes it as its own
parameter; what remains is the engine types the facade itself names
(`StoreTelemetry`'s members, the ledger's snapshot) and the write-path helpers
with no facade form (`IngestPipeline`'s `mapBounded`, `QUERY_FANOUT` and
`IngestStats.timed`, `SyncEngine`'s stage reads). Reach-through via
`VespaEventStore.store` is not guarded: `store` is the name of every
`IEventStore` here, and source text cannot tell the hatch from the interface.

**`TorTransport`'s tests live with `TorTransport`.** They were in `:monitor`,
in `:peers`' package, because three of the sixteen turn on `shouldPreProbe` and
`Unreachability`, which are the monitor's. Twelve moved to `:peers`; the four
that are about what this plane probes and what it may publish stayed, in
`:monitor`'s own package, with a SOCKS fake that refuses instead of one that
records. Sharing one fake across the boundary would have meant a test-fixtures
source set to hold a class two tests want for opposite reasons.
