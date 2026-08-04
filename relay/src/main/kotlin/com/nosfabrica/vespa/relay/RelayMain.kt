/*
 * Copyright (c) 2026 NosFabrica
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
 * Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN
 * AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package com.nosfabrica.vespa.relay

import com.nosfabrica.vespa.eventstore.store.SchemaDeployer
import com.nosfabrica.vespa.eventstore.store.VespaEventStore
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.server.RelayServerListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import java.net.URI

/**
 * Run a standalone trust-ranking Nostr relay against a Vespa. It opens the
 * store, serves the NIP-50 relay + NIP-11 doc, and blocks. When a router config
 * is set it also mirrors events from upstream relays into that store (see the
 * router env vars below); large-scale crawling/trust-sync remains a separate
 * concern, for which SoT (github.com/vitorpamplona/sot) is the reference app.
 *
 * Configuration is entirely from the environment:
 *
 *   VESPA_URL           the Vespa query endpoint          (default http://localhost:8080)
 *   RELAY_PORT          the port to listen on             (default 7777)
 *   RELAY_URL           this relay's own ws url — its NIP-42 identity and NIP-62
 *                       vanish scope                      (REQUIRED)
 *   AUTO_DEPLOY         deploy the bundled schema on EVERY boot (default true),
 *                        so the cluster always matches the schema this build
 *                        expects. A no-change deploy is a cheap no-op; a failed
 *                        one is fatal only when Vespa has no schema to fall back on
 *   VESPA_CONFIG_URL     Vespa's config server (default: VESPA_URL's host on :19071)
 *
 *   NIP-11 identity:
 *   RELAY_NAME / RELAY_DESCRIPTION / RELAY_ICON / RELAY_BANNER /
 *   RELAY_CONTACT_PUBKEY / RELAY_CONTACT (human contact) /
 *   RELAY_VERSION (override the build version) / RELAY_POSTING_POLICY /
 *   RELAY_PRIVACY_POLICY / RELAY_TERMS_OF_SERVICE
 *
 *   Protection limits (each optional; sane defaults otherwise, see RelayConfig):
 *   MAX_MESSAGE_LENGTH / MAX_SUBSCRIPTIONS / MAX_FILTERS / MAX_LIMIT /
 *   DEFAULT_LIMIT / MAX_SUBID_LENGTH / MAX_EVENT_TAGS / MAX_CONTENT_LENGTH /
 *   MIN_POW_DIFFICULTY / CREATED_AT_LOWER_LIMIT / CREATED_AT_UPPER_LIMIT
 *
 *   NIP-77 negentropy tuning (optional; strfry-parity defaults):
 *   NEG_FRAME_SIZE_LIMIT / NEG_MAX_SYNC_EVENTS / NEG_MAX_SESSIONS_PER_CONNECTION
 *
 *   NIP-86 relay management (optional):
 *   RELAY_ADMIN_PUBKEYS   comma/space-separated 64-hex admin keys; empty ⇒ off
 *   RELAY_HTTP_URL        the http(s) url NIP-98 auth must be tagged with
 *                         (default: RELAY_URL with ws→http, wss→https)
 *
 *   Router / upstream mirror (optional; see RouterConfig / MirrorRouter):
 *   ROUTER_CONFIG            strfry-style `streams { }` config (HOCON), inline
 *   ROUTER_CONFIG_FILE       path to a file holding that config
 *                            (how far back a stream reaches is its filter's own
 *                            `since`/`until` — absent means the whole history)
 *   ROUTER_DYNAMIC_*         defaults for `relaySource = [...]` streams, whose relay
 *                            list is read from the store's own 10002s/10040s
 *                            rather than configured (see RelaySource)
 *
 *   Trust view (see TrustProjection.reconcile):
 *   REINDEX_FTS_ON_START     re-derive every event's search fields once, in the
 *                            background (default false). Needed after a store
 *                            upgrade that changes SearchExtractors or adds fed
 *                            search fields; walks the whole corpus
 *   SWEEP_ORPHAN_SCORES_ON_START  delete every kind-30382 signed by a service no
 *                            stored 10040 names — cards that rank nothing and are
 *                            read by nobody, which a by-kind 30382 sync accrues by
 *                            the million. Any value other than `true` is a DRY RUN
 *                            (one grouping query, no writes); unset ⇒ off. An
 *                            operator action: pair it with narrowing the sync, or
 *                            the next walk re-downloads what it freed
 *   TRUST_RECONCILE_ON_START  at startup, re-derive any service whose scores are
 *                             not projected under its current observer. Needed
 *                             because the view is maintained by write triggers
 *                             that a duplicate never reaches (default true)
 *
 *   Parse audit / quartz logging (optional; see ParseAudit):
 *   SERVING_PRESSURE_THRESHOLD_MS  mean client-read latency (default 2000) above
 *                            which the mirror yields between batches, so a sync
 *                            cannot starve the clients this relay exists for
 *   ROUTER_WIRE_LOG          "" (default) logs only what the relay complains
 *                            about — NOTICE, CLOSED, failed sends. "sent" adds
 *                            every command we send; "full" adds every message
 *                            received, which is a line per event
 *   QUARTZ_LOG_LEVEL         quartz's log floor: DEBUG/INFO/WARN/ERROR. Quartz
 *                            defaults to DEBUG, so malformed upstream profiles
 *                            log a line per event during a backfill
 *   PARSE_AUDIT_FILE         collect what quartz cannot parse into a grouped JSON
 *                            report here, with raw sample events; unset ⇒ off
 *   PARSE_AUDIT_SAMPLES      raw events kept per distinct failure (default 5)
 *   PARSE_AUDIT_INTERVAL_SECONDS  report rewrite interval (default 60)
 *
 *   Router resume state (optional; see SyncCursors):
 *   ROUTER_SYNC_STATE_FILE   where the per-(relay, filter) synced `created_at`
 *                            band is kept, so a relay without NIP-77 is not
 *                            re-read from scratch on every restart; unset ⇒
 *                            in memory only
 */
fun main() {
    val env = System.getenv()
    val vespaUrl = env["VESPA_URL"] ?: "http://localhost:8080"
    val port = env["RELAY_PORT"]?.toIntOrNull() ?: 7777
    val relayUrlRaw = env["RELAY_URL"] ?: error("RELAY_URL is required — this relay's own ws url (NIP-42 identity / NIP-62 vanish scope).")
    val relayUrl =
        RelayUrlNormalizer.normalizeOrNull(relayUrlRaw)
            ?: error("RELAY_URL '$relayUrlRaw' is not a valid relay url.")
    val autoDeploy = env["AUTO_DEPLOY"]?.toBooleanStrictOrNull() ?: true

    // This relay's own keypair, in every role where it acts as itself: the NIP-11
    // `self` it advertises, the NIP-42 challenges it answers, the NIP-66 liveness
    // it publishes. Read first so a malformed key stops the process here, with a
    // clear message, rather than surfacing hours later as upstreams that
    // mysteriously serve nothing. Unset ⇒ the relay acts anonymously.
    val identity = RelayIdentity.fromEnv { env[it] }
    if (identity != null) {
        System.err.println("relay identity: ${identity.pubKey.take(12)}… (NIP-11 self, NIP-42 auth, NIP-66 monitor)")
    }

    val limits = relayLimitsFromEnv(env)
    val negentropy = negentropySettingsFromEnv(env)
    val rejectFutureSeconds = rejectFutureSecondsFromEnv(env)

    // NIP-86 is enabled only when at least one valid admin key is configured;
    // its ban lists persist to RELAY_STATE_FILE when set.
    val adminPubkeys = adminPubkeysFromEnv(env)
    val banStore = if (adminPubkeys.isNotEmpty()) openBanStore(env["RELAY_STATE_FILE"]) else null

    val listener =
        if (env["LOG_CONNECTIONS"]?.toBooleanStrictOrNull() == true) {
            ConnectionCountListener()
        } else {
            RelayServerListener.None
        }

    // Deploy the schema this build EXPECTS, every boot — not only when Vespa has
    // no application at all.
    //
    // VespaEventStore.open's own autoDeploy is deployIfAbsent: a fresh cluster
    // gets the schema, one already serving is left alone. That is safe for a
    // first run and wrong for an upgrade, because the schema travels with the
    // store jar while the cluster keeps whatever it was given months ago. When
    // the two drift, Vespa answers every write with
    //
    //   Status 400 ... Field 'name_parts' is not defined in document type 'event'
    //
    // and the router counts it, drops the event and carries on. That cost
    // 2,336,288 events in one run before anybody noticed, and they are not
    // recoverable — a 400 is permanent and nothing re-offers them.
    //
    // Deploying unconditionally makes the running cluster match the code that is
    // talking to it. Vespa handles a no-change deploy as a cheap no-op (a new
    // session that activates with no configChangeActions), so the cost of the
    // common case is one request at startup.
    val configUrl = env["VESPA_CONFIG_URL"] ?: configUrlFor(vespaUrl)
    if (autoDeploy) {
        System.err.println("schema: deploying the bundled application package to $configUrl")
        deployBundledSchema(vespaUrl, configUrl)
        System.err.println("schema: deployed and serving")
    }

    val store = VespaEventStore.open(vespaUrl, relay = relayUrl, autoDeploy = false, configUrl = configUrl)

    // The trust view is derived on WRITE, and dedup drops an event the store
    // already holds before the projection sees it — so a corpus mirrored before
    // its 10040s arrived stays unprojected, and every ranked search comes back
    // empty with nothing logged anywhere. Settling it here costs a few queries
    // when the answer is "nothing to do", which is the normal case.
    // Started here, AWAITED NOWHERE. This used to be `runBlocking`, sitting
    // between opening the store and starting the listener, and the store's own
    // note for it — "a few queries when the answer is nothing to do, which is
    // the normal case" — stopped being true as the corpus grew. Measured at 12+
    // minutes on 36M kind-30382 events, with Vespa at 356% CPU and this relay
    // serving NOTHING: no websocket, no router, no NIP-11. Every restart was an
    // outage that got longer the better the relay did its job.
    //
    // The work is worth doing and none of it needs to happen before the first
    // client connects. Serving with an unsettled projection degrades one
    // feature — ranked search returns less until it lands — where blocking
    // degrades all of them, absolutely, for an unbounded time. So it runs
    // behind the server and says where it has got to.
    val trustScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    // A one-off migration, not a boot step — hence opt-in and off by default.
    //
    // Which kinds are searchable, and how SearchExtractors decomposes them, is
    // baked into the build. A store fed by older code can be stale or missing
    // from search entirely until this re-derives it, and it is also the RE-FEED
    // that backfills the near-tier prefix/fuzzy arrays: those are fed fields, so
    // a Vespa reindex cannot produce them — only a put can.
    //
    // In the background and NOT awaited, like the trust reconcile. It walks the
    // whole corpus (54M events here) taking the writer lock a page at a time, so
    // as a startup barrier it would be an outage measured in hours. Progress is
    // printed because a silent hours-long job is indistinguishable from a hung
    // one — this router has already taught that lesson twice.
    //
    // Ordering is already correct by construction: the schema deploy above runs
    // before the store opens, and the store's own note requires exactly that —
    // the backfill re-puts docs carrying the near fields, and a serving schema
    // that predates them rejects those puts outright.
    if (env["REINDEX_FTS_ON_START"]?.toBooleanStrictOrNull() == true) {
        trustScope.launch {
            val startedMs = System.currentTimeMillis()
            println("fts: reindexing the whole corpus in the background — search results may be incomplete until it finishes")
            var cursor: String? = null
            var total = 0L
            var pages = 0
            // The denominator, asked for once. A rising count with nothing to
            // measure it against does not read as progress — it reads as
            // something repeating, which is exactly how the first version of
            // this line was received. Null rather than a guess if it fails: an
            // unknown denominator is better than a wrong one.
            val expected = runCatching { store.count(Filter()) }.getOrNull()?.toLong()
            // Where the walk got to, on disk. The store's API is resumable by
            // design — the cursor is an opaque token meant to be persisted and
            // handed back — and holding it only in memory threw away 12,254,483
            // events the first time a page failed. Beside the sync cursors, for
            // the same reason those are there.
            val cursorFile = env["FTS_CURSOR_FILE"] ?: "/var/lib/vespa-relay/fts-cursor.txt"
            cursor =
                runCatching {
                    java.io
                        .File(cursorFile)
                        .takeIf { it.isFile }
                        ?.readText()
                        ?.trim()
                        ?.ifBlank { null }
                }.getOrNull()
            if (cursor != null) println("fts: resuming from a saved cursor")
            try {
                do {
                    // A page can fail for reasons that are nothing to do with
                    // this page: Vespa answered one with an HTML error body
                    // ("Unexpected character ('<')") while under memory
                    // pressure, and the whole walk died on it. Retry the SAME
                    // cursor a few times before giving up, so a busy engine
                    // costs seconds rather than hours of redone work.
                    var p: com.vitorpamplona.quartz.nip01Core.store.FtsReindexProgress? = null
                    var attempt = 0
                    while (p == null) {
                        p =
                            runCatching { store.reindexFullTextSearch(cursor) }
                                .onFailure { e ->
                                    if (++attempt > FTS_PAGE_RETRIES) throw e
                                    System.err.println("fts: page failed (${e.message?.take(80)}) — retry $attempt/$FTS_PAGE_RETRIES in ${attempt * 5}s")
                                }.getOrNull()
                        if (p == null) delay(attempt * 5_000L)
                    }
                    cursor = p.cursor
                    runCatching { java.io.File(cursorFile).writeText(cursor ?: "") }
                    total += p.processedThisBatch
                    // Every page would be a flood; never would be silence.
                    if (++pages % 50 == 0) {
                        val secs = (System.currentTimeMillis() - startedMs) / 1000
                        val rate = if (secs > 0) total / secs else 0
                        val pct = expected?.takeIf { it > 0 }?.let { " (${total * 100 / it}%)" } ?: ""
                        val eta =
                            if (expected != null && rate > 0 && expected > total) {
                                ", ETA ~${fmtDuration((expected - total) / rate * 1000)}"
                            } else {
                                ""
                            }
                        println(
                            "fts: reindexed ${total}${expected?.let { "/$it" } ?: ""} event(s)$pct" +
                                " in ${fmtDuration(secs * 1000)}, $rate/s$eta",
                        )
                    }
                } while (!p.done)
                println("fts: reindex complete — $total event(s) in ${fmtDuration(System.currentTimeMillis() - startedMs)}")
                // Done means done: leaving the cursor would resume a finished
                // walk from its tail on the next boot with the flag still set.
                runCatching { java.io.File(cursorFile).delete() }
            } catch (e: Exception) {
                // Resumable by design, so say where it stopped rather than only
                // that it did.
                System.err.println(
                    "fts: reindex FAILED after $total event(s): ${e.message}" +
                        " — the cursor is saved, so restarting with REINDEX_FTS_ON_START resumes here",
                )
            }
        }
    }
    // DELETE every kind-30382 signed by a service no stored 10040 names.
    //
    // This relay is exactly the case the store wrote the sweep for: the
    // `nosfabricaScores` stream asks for kind 30382 with no author narrowing, so
    // it pulls every service publishing on that relay — 87 of them — where the
    // 10040s we hold name a few. Those cards can never become a tensor cell for
    // any observer, so they rank nothing and are read by nobody, and on this
    // machine they are the difference between Vespa fitting in its 34g and
    // sitting on the ceiling at 99% with ingest wedged behind it.
    //
    // A deletion is not a tombstone: the same by-kind stream re-downloads what
    // this frees on its next walk. Reclaiming space here and narrowing that
    // filter are one job, not two.
    //
    // Dry run by default when the value is not `true` — "which services, how
    // many cards" costs one grouping query and no writes, and a sweep that
    // deletes on a typo is not a sweep anyone should have to think twice about.
    env["SWEEP_ORPHAN_SCORES_ON_START"]?.trim()?.takeIf { it.isNotEmpty() }?.let { setting ->
        val dryRun = setting.toBooleanStrictOrNull() != true
        trustScope.launch {
            val startedMs = System.currentTimeMillis()
            println(
                "sweep: ${if (dryRun) "DRY RUN — no writes" else "DELETING orphan scores"}" +
                    " — kind 30382 from services no stored 10040 names",
            )
            var lastReport = 0L
            runCatching {
                store.sweepOrphanScores(dryRun) { done, totalServices, swept, totalScores ->
                    // Paced, because the callback fires per page and this walks
                    // millions of cards. The totals come from the store's own
                    // grouping query, so this is a real fraction rather than the
                    // downloaded/downloaded shape that once printed 100% for
                    // hours.
                    val now = System.currentTimeMillis()
                    if (now - lastReport >= 15_000) {
                        lastReport = now
                        val pct = if (totalScores > 0) " (${swept * 100L / totalScores}%)" else ""
                        println("sweep: $done/$totalServices service(s), $swept/$totalScores score(s)$pct")
                    }
                }
            }.onSuccess { report ->
                val secs = (System.currentTimeMillis() - startedMs) / 1000
                // Counts and three examples, NOT the report's own toString: that
                // carries every orphan pubkey and printed a 38,920-character log
                // line, which is a wall rather than a number. `orphans` is
                // deliberately complete so a caller can act on it — a log line
                // is not that caller.
                if (report.refused) {
                    println(
                        "sweep: REFUSED — no readable 10040 attribution, so every score would look orphaned." +
                            " Nothing was touched; mirror a provider list first",
                    )
                } else {
                    val eg = report.orphans.take(3).joinToString { it.take(8) + "…" }
                    println(
                        "sweep: ${if (dryRun) "would delete" else "deleted"} ${report.scoresSwept} score(s)" +
                            " from ${report.orphans.size} orphan service(s) of ${report.servicesSeen} seen" +
                            (if (report.remapped.isNotEmpty()) ", ${report.remapped.size} remapped mid-sweep and left alone" else "") +
                            " in ${secs}s" +
                            (if (eg.isNotEmpty()) " (e.g. $eg)" else "") +
                            if (dryRun) " — set SWEEP_ORPHAN_SCORES_ON_START=true to apply" else "",
                    )
                }
            }.onFailure { e ->
                println("sweep: FAILED after ${(System.currentTimeMillis() - startedMs) / 1000}s: ${e.message}")
            }
        }
    }
    if (env["TRUST_RECONCILE_ON_START"]?.toBooleanStrictOrNull() != false) {
        trustScope.launch {
            println("trust: reconciling in the background — ranked search may return less until this finishes")
            val startedMs = System.currentTimeMillis()
            reconcileTrustWithRetry(store)
            println("trust: background reconcile finished in ${(System.currentTimeMillis() - startedMs) / 1000}s")
        }
    }
    // One instance, shared: the relay server measures client reads into it, the
    // router reads it back to decide whether to yield. A relay answers clients
    // first and mirrors with what is left.
    val servingPressure = ServingPressure(thresholdMs = env["SERVING_PRESSURE_THRESHOLD_MS"]?.trim()?.toLongOrNull()?.coerceAtLeast(100) ?: 2_000)
    val relay =
        NostrRelayServer(
            store = store,
            servingPressure = servingPressure,
            relayUrl = relayUrl,
            listener = listener,
            limits = limits,
            negentropySettings = negentropy,
            banStore = banStore,
            pubkeyAllow = allowPubkeysFromEnv(env),
            pubkeyDeny = denyPubkeysFromEnv(env),
            kindAllow = allowKindsFromEnv(env),
            kindDeny = denyKindsFromEnv(env),
            rejectFutureSeconds = rejectFutureSeconds,
        )

    // Prune NIP-40 expired events on a schedule (the store schedules nothing itself).
    val sweeper = ExpirationSweeper(store, expirationSweepSecondsFromEnv(env)).start()

    // The router (strfry-style mirror): when ROUTER_CONFIG / ROUTER_CONFIG_FILE is
    // set, keep live subscriptions open against the configured upstream relays and
    // mirror their events of the configured kinds into this same store. Fills what
    // the search serves, from the network. Unset ⇒ serve-only.
    // Opt-in diagnostic: collect the events quartz cannot fully parse, with their
    // raw JSON, so the gaps can be fixed upstream. Also the knob that quiets
    // quartz's own logging, whose floor defaults to DEBUG.
    val parseAudit = ParseAudit.installFromEnv(env)

    // Where a paged relay's already-walked history is remembered, so a restart
    // resumes instead of re-reading the corpus. Unset ⇒ in memory, which is the
    // same as not having it.
    val cursors = SyncCursors.fromEnv(env)

    val router =
        RouterConfigLoader.fromEnv(env)?.let {
            MirrorRouter(
                store,
                it,
                audit = parseAudit,
                cursors = cursors,
                signer = identity,
                wireLogMode = env["ROUTER_WIRE_LOG"]?.trim()?.lowercase() ?: "",
                servingPressure = servingPressure,
            ).start()
        }

    val admin =
        banStore?.let {
            Nip86Admin(
                banStore = it,
                adminPubkeys = adminPubkeys,
                relayHttpUrl = env["RELAY_HTTP_URL"] ?: relayUrlRaw.httpFromWs(),
                // Banning a source also drops what it already published.
                purge = { filter -> store.delete(filter) },
            )
        }

    Runtime.getRuntime().addShutdownHook(
        Thread {
            // The background reconcile holds the store open and would keep
            // querying an engine we are about to close. Cancelled first, and NOT
            // waited for: an unfinished reconcile costs a less complete ranking
            // until the next start, which is exactly what it costs anyway.
            trustScope.cancel()
            // Stop mirroring into the store before the relay and store close.
            router?.close()
            // After the router, so the final report includes the last batch.
            parseAudit?.close()
            sweeper.close()
            relay.close()
            store.close()
        },
    )

    println(
        "vespa-relay listening on :$port  (vespa $vespaUrl, relay $relayUrl)" +
            (if (admin != null) "  [NIP-86 admin: ${adminPubkeys.size} key(s)]" else "") +
            (
                if (router != null) {
                    "  [router: mirroring ${router.upstreamCount()} relay(s)" +
                        (if (router.dynamicStreamCount() > 0) " + ${router.dynamicStreamCount()} dynamic stream(s)" else "") +
                        "]"
                } else {
                    ""
                }
            ),
    )
    serveRelay(
        relay = relay,
        port = port,
        nip11 =
            Nip11Info(
                name = env["RELAY_NAME"] ?: "vespa-relay",
                description = env["RELAY_DESCRIPTION"],
                icon = env["RELAY_ICON"],
                banner = env["RELAY_BANNER"],
                contactPubkey = PubKeys.decodeOrNull(env["RELAY_CONTACT_PUBKEY"], "RELAY_CONTACT_PUBKEY"),
                // Derived, never declared: a pubkey an operator types in is an
                // assertion no reader can check, while this one is provable
                // against every 22242 and 30166 the relay signs.
                selfPubkey = identity?.pubKey,
                contact = env["RELAY_CONTACT"],
                version = env["RELAY_VERSION"],
                postingPolicy = env["RELAY_POSTING_POLICY"],
                privacyPolicy = env["RELAY_PRIVACY_POLICY"],
                termsOfService = env["RELAY_TERMS_OF_SERVICE"],
            ),
        limits = limits,
        admin = admin,
        // The bundled web UI (a NIP-50 client) — served on a plain browser GET.
        landingPage = webUi(),
        statsPage = kindStatsUi(),
        observerStatsPage = observerStatsUi(),
    )
}

/**
 * Reconcile the trust view, waiting out an engine that is not answering yet.
 *
 * A cold Vespa serves its config port within seconds and its QUERIES only once
 * the content node has loaded the index, which on a large corpus is minutes. A
 * relay that starts in that gap gets a 503, and a reconcile that gave up there
 * would skip the repair for the whole life of the process — the same shape as the
 * bug it exists to fix: a repair that runs once and can never run again. So a
 * failure is treated as "not yet" and retried, rather than as an answer.
 *
 * Bounded, because a failure that is NOT warm-up (a wrong url, a dead cluster)
 * must not hold the relay off its port forever. When the budget runs out the
 * relay serves anyway and says what that costs, since serving unranked results
 * beats serving nothing.
 */
private suspend fun reconcileTrustWithRetry(store: VespaEventStore) {
    var waited = 0L
    var attempt = 0
    while (true) {
        attempt++
        val result = runCatching { store.reconcileTrust() }
        result.onSuccess { r ->
            when {
                // Said "consistent" once when the store had 24M events and the
                // engine had simply not finished loading them. Zero providers is
                // either a fresh relay or a store that is not answering properly
                // yet — never a clean bill of health, so it does NOT end the wait.
                //
                // Returning here is what left a relay with 270 kind-10040s and an
                // empty projection for hours: the call had succeeded, one second
                // after start, against a content node still loading 24M events.
                // Zero is indistinguishable from cold, so it is retried like cold
                // and only accepted once the budget is spent.
                r.services == 0 -> {
                    if (waited >= TRUST_RECONCILE_MAX_WAIT_MS) {
                        println(
                            "trust: still no provider lists after ${waited / 1000}s — nothing to project. " +
                                "A fresh relay: ranking stays empty until a kind 10040 arrives and this runs again.",
                        )
                        return
                    }
                    if (attempt == 1) {
                        println("trust: no provider lists yet; waiting for the engine to serve its corpus before ranking is usable")
                    }
                    delay(TRUST_RECONCILE_RETRY_MS)
                    waited += TRUST_RECONCILE_RETRY_MS
                    return@onSuccess
                }

                r.isClean() -> {
                    println("trust: ${r.services} service(s) checked, projection consistent")
                    return
                }

                else -> {
                    println("trust: re-derived ${r.rebuilt.size} of ${r.services} service(s) whose scores were unprojected")
                    return
                }
            }
        }
        if (result.isSuccess) continue
        val cause = result.exceptionOrNull()
        if (waited >= TRUST_RECONCILE_MAX_WAIT_MS) {
            System.err.println(
                "trust: reconcile still failing after ${waited / 1000}s; " +
                    "serving with the projection as-is — ranked searches may return nothing until it runs clean",
            )
            // The WHOLE stack, once. A message alone says a failure happened and
            // nothing about where: an `IndexOutOfBoundsException: Index: 1, Size:
            // 1` from this path cost a day of inference that a single frame would
            // have ended. This is the only place it can be printed — the throwable
            // is caught here and goes no further.
            cause?.printStackTrace()
            return
        }
        if (attempt == 1) {
            println("trust: engine not answering yet (${cause?.message?.take(80)}); waiting for it before ranking is usable")
            // Also on the FIRST failure, not only after the budget runs out. A
            // deterministic bug and a cold engine look identical from one
            // message, and waiting ten minutes to tell them apart is ten minutes
            // of a relay that is not serving.
            cause?.printStackTrace()
        }
        delay(TRUST_RECONCILE_RETRY_MS)
        waited += TRUST_RECONCILE_RETRY_MS
    }
}

// The engine is being waited ON, not polled at: a cold content node takes
// minutes, and each attempt is a real query.
private const val TRUST_RECONCILE_RETRY_MS = 5_000L
private const val TRUST_RECONCILE_MAX_WAIT_MS = 10 * 60 * 1000L

/**
 * Deploy the bundled application package and wait until Vespa serves it.
 *
 * A deploy failure against a Vespa that is already serving keeps the relay up
 * on the schema it has (with a warning naming the cost: fields the schema
 * lacks stay rejected) — a config server that is unreachable, say, must not
 * take down a relay that ran fine yesterday. On a fresh Vespa the failure
 * rethrows: there is no schema to fall back to, so nothing could serve anyway.
 */
private fun deployBundledSchema(
    vespaUrl: String,
    configUrl: String,
) {
    val deployer = SchemaDeployer(configUrl)
    try {
        deployer.deploy()
    } catch (e: Exception) {
        if (!deployer.isServing(vespaUrl)) throw e
        System.err.println(
            "relay: schema deploy to $configUrl failed (${e.message?.take(200)}); " +
                "serving on the schema Vespa already has — writes carrying fields it lacks will be rejected until a deploy succeeds",
        )
    }
    deployer.awaitServing(vespaUrl)
}

/** The config server sits on :19071 by convention, on the same host as the query endpoint. */
private fun configUrlFor(queryUrl: String): String {
    val u = URI.create(queryUrl)
    return URI(u.scheme, null, u.host, 19071, null, null, null).toString()
}

/** Map a ws/wss url to its http/https origin for NIP-98's `u` tag. */
private fun String.httpFromWs(): String =
    when {
        startsWith("wss://") -> "https://" + substring(6)
        startsWith("ws://") -> "http://" + substring(5)
        else -> this
    }

/** The bundled search UI (`resources/index.html`), or null if it isn't on the classpath. */
private fun webUi(): String? = object {}.javaClass.getResource("/index.html")?.readText()

/** The bundled per-kind COUNT page (`resources/kind_stats.html`). */
private fun kindStatsUi(): String? = object {}.javaClass.getResource("/kind_stats.html")?.readText()

/** The bundled observer sync-check page (`resources/observer_stats.html`). */
private fun observerStatsUi(): String? = object {}.javaClass.getResource("/observer_stats.html")?.readText()

/**
 * Retries for ONE page of the full-text reindex before the walk gives up.
 *
 * A page can fail for reasons unrelated to its contents — Vespa answered one
 * with an HTML error body under memory pressure, and the walk died 12.2M events
 * in. Five attempts with a widening gap turns that into a pause.
 */
private const val FTS_PAGE_RETRIES = 5
