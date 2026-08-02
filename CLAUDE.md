# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A standalone Nostr relay serving trust-ranked NIP-50 search, backed by [Vespa](https://vespa.ai).
This repo is deliberately thin: the Nostr protocol engine (`RelayServerBase`, policies, NIP-77
negentropy) lives in **quartz** (the Amethyst repo, via JitPack) and the Vespa-backed store lives
in **vespa-eventstore** (also JitPack). What lives *here* is the composition and serving layer:
env-var configuration, the Ktor websocket mount, the NIP-11 doc, NIP-86 admin, and the router
that mirrors events from upstream relays.

## Commands

```bash
./gradlew build                  # compile + all tests + spotlessCheck — the CI gate
./gradlew :relay:test            # tests only
./gradlew :relay:test --tests "com.vitorpamplona.quartz.eventstore.relay.RelayProtocolTest"
./gradlew spotlessApply          # fix formatting (ktlint + license header) — run before committing
./gradlew :relay:run             # run against RELAY_URL / VESPA_URL from the environment
docker compose up --build        # single-node Vespa + the relay on ws://localhost:7777
```

JDK 21, Kotlin 2.4. Tests are JUnit Platform with `kotlin.test` assertions.

The build auto-installs git hooks from `.git-hooks/`: **pre-commit runs `spotlessCheck`,
pre-push runs the tests.** A commit rejected for formatting means run `spotlessApply` and
re-commit. Spotless also enforces the MIT license header from `.spotless/copyright.kt` on
every `.kt` file — new files get it added by `spotlessApply`.

Tests need **no running Vespa**: `RelayProtocolTest` and friends drive the real protocol
engine over `InMemoryEventIndex`, production code that ships in vespa-eventstore's `vespa`
engine jar.

## Dependency pinning — the one build gotcha

`gradle/libs.versions.toml` pins quartz and vespa-eventstore **by JitPack commit hash**.
Gradle resolves version conflicts by picking the lexicographically "higher" string, which is
meaningless for hashes — so `relay/build.gradle.kts` forces the quartz pin with
`resolutionStrategy { force(libs.quartz) }`. When bumping quartz, change the hash in the
version catalog; without the force, vespa-eventstore's transitive quartz could silently win
and you'd compile against the wrong commit with no warning.

## Architecture

Everything is one Gradle module (`:relay`) and one flat package,
`com.vitorpamplona.quartz.eventstore.relay` (~19 files). The wiring:

- **`RelayMain`** — the composition root and entrypoint. Reads all configuration from
  environment variables (its KDoc is the full reference, mirrored in README tables), opens
  the `VespaEventStore`, deploys the bundled Vespa schema on boot (`AUTO_DEPLOY`), builds
  the server, starts the optional router, and blocks.
- **`RelayConfig`** — env parsing. The style is deliberate: a malformed pubkey/nsec/ban
  entry **fails startup** rather than being dropped, because a ban that isn't enforced looks
  identical to one never configured.
- **`NostrRelayServer`** — quartz's `RelayServerBase` + `LiveEventStore` over the store,
  with the per-connection policy stack (auth verification, allow/deny by pubkey and kind,
  NIP-86 ban lists, future-event rejection). EVENT signatures are verified in the
  `IngestQueue` off the hot path, not in a policy. `ObserverRoutingBackend` resolves the
  observer per REQ/COUNT — the NIP-42-authenticated pubkey, else `DEFAULT_OBSERVER` — which
  is what makes search rank by the *caller's* web of trust.
- **`RelayApp`** / **`RelayRoute`** — the Ktor layer. `serveRelay(...)` binds a port
  batteries-included; `Route.nostrRelay(relay)` mounts the pieces in an existing server
  (the relay is embeddable — keep that path working). One port serves the websocket,
  the NIP-11 doc (`Accept: application/nostr+json`), the NIP-86 RPC (`POST /`), and the
  web UI (`resources/index.html`).
- **`MirrorRouter`** + **`RouterConfig`** — the upstream mirror: strfry-style `streams { }`
  HOCON, live subscriptions plus NIP-77 negentropy backfill (paged REQ fallback for relays
  without it), dynamic relay lists via `relaySource` scans. **`SyncCursors`** persists the
  per-(relay, filter) covered `created_at` band so paged relays aren't re-downloaded every
  restart; **`StreamPhase`** tracks backfill progress/ETA logging.
- **`RelayState`** — NIP-86 ban/allow lists, persisted to JSON when `RELAY_STATE_FILE` is set.
- **`RelayIdentity`** — the relay's own keypair (`RELAY_NSEC`): NIP-11 `self`, answering
  NIP-42 challenges as a client, signing NIP-66 liveness records (`RelayHealth`/`RelayDiscovery`).
- **`ParseAudit`** — opt-in report of events quartz cannot parse, collected during mirroring.

The store is owned by the composition root, not the server — `NostrRelayServer.close()`
deliberately does not close it, because the router shares it.

## Conventions

- Comments and KDoc here explain *why* — constraints, failure modes that were actually hit,
  and trade-offs — not what the next line does. Match that register; don't strip them.
- Configuration is **only** via environment variables. A new knob needs: `RelayConfig`
  parsing, the `RelayMain` KDoc, the README table, and `.env.example` / `docker-compose.yml`
  if operators would set it.
- `router.conf.example` is executable documentation: `RouterConfExamplesTest` parses it and
  asserts structural claims (e.g. static seed streams exist for the dynamic scans to read
  from). Edit the example and the test together.
- `tools/profile-search-compare/` is a dependency-free Node harness comparing this relay's
  profile search against a reference relay; it is not part of the Gradle build.
