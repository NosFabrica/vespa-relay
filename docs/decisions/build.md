# Build decisions

The history behind the Gradle files.

**Every module forces the quartz pin.** Both quartz and vespa-eventstore are
JitPack commit hashes, and Gradle resolves a version conflict by picking the
lexicographically higher string. Pinning quartz to 6d518adddb while the store
carried 79f198c729 silently resolved to the store's, because '7' > '6', and
the build compiled against a quartz without the method being added. Nothing
warned.

**The JavaScript suite is a Gradle task on `check`.** It was a README command
and ran nowhere automatically for as long as that was true: twenty suites over
about 14,000 lines of browser code, executed when somebody remembered. Five
wrong denominators once shipped behind a single card. It fails rather than
skips when node is absent, because a suite that stands down silently lets the
build go green over code it never ran.

**The checkout-reading guards have their own task.** `ModuleBoundariesTest` and
the two beside it declare every build file and every source tree as inputs, or
Gradle calls them up to date after exactly the change they exist to catch.
Attached to `:common:test`, that made this module's whole suite re-run whenever
any module's browser code changed. `archTest` carries the inputs; `test`
excludes the package and turns on this module's own sources alone.

**Probe switches are forwarded by hand and the list is tested.** A system
property on the Gradle command line reaches the daemon, not the forked test
JVM. A probe whose switch is not forwarded prints its own "[skip]" line, which
reads exactly like a probe nobody asked for; that happened on the first run of
`RelayListLiveProbe`. `ProbeSwitchesAreForwardedTest` holds the list, in
`:common` with the other guards that read the checkout rather than run it; the
module graph and the package map are `docs/decisions/module-boundaries.md`.

**Module conventions live in `build-logic/`.** The Kotlin plugin, JVM 21, JUnit,
`kotlin("test")`, the `-PtestHeap` opt-in and the quartz force were copied into
six build files, and the trap list's "a new module must add its own force" was
enforced by copy-paste alone. One precompiled plugin, `vespa-relay.kotlin-module`,
now carries them, and `forwardProbeSwitches(...)` replaced roughly sixty
hand-written `System.getProperty(x)?.let { systemProperty(x, it) }` lines;
`ProbeSwitchesAreForwardedTest` reads that call's argument list.

**The catalog holds pins, not their history.** About 1,190 of the 1,253 lines
in `libs.versions.toml` were notes on past bumps, which made every bump a large
diff. They moved verbatim to `dependency-pins.md`; the catalog keeps one line on
each pin that a bump must not forget.

**Hooks are installed 0755, where git reads them, and never under CI.** They
were installed world-writable, into a linked worktree's private gitdir (which
git never reads hooks from), and on every compile in CI and image builds.
pre-push runs `check`, so `archTest` and `jsTest` gate a push the way CI does.

**The image runs JDK 27; the build stays on 21.** Bytecode for 21 runs unchanged on a newer JVM, so only the
Dockerfile's runtime stage moved. 27 turns compact object headers on by default, and with them a live feed's
heap fell from 7.65 to 6.97 KB per idle connection. CPU per frame and delivery latency on 21, 25 and 27,
with headers on and off, were within each other's noise (docs/live-feeds.md). 27 had published only moving
tags, so the image is pinned by digest. `--enable-native-access=ALL-UNNAMED` is in both start scripts and in
the CDS dump: Netty loads its own native libraries, which JDK 24+ warns about at every boot without it, and
the archive is dumped under the module options it runs with.

