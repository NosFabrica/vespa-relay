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

**Probe switches are forwarded by hand and the list is tested.** A system
property on the Gradle command line reaches the daemon, not the forked test
JVM. A probe whose switch is not forwarded prints its own "[skip]" line, which
reads exactly like a probe nobody asked for; that happened on the first run of
`RelayListLiveProbe`. `ProbeSwitchesAreForwardedTest` holds the list.
