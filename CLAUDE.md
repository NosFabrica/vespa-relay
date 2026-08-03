# CLAUDE.md

See **[AGENTS.md](AGENTS.md)** — one file, so the two cannot drift apart.

It covers the build and test commands, the module layout, how the router's
streams and sync cursors work, the instrumentation to reach for before forming a
theory, the commenting and testing conventions this codebase holds itself to,
and the traps that have already cost real time (JitPack pins resolving
lexicographically, stacked KDoc failing ktlint, verifying a fix while the system
is idle).
