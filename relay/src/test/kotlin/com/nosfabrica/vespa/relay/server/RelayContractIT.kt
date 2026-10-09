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
package com.nosfabrica.vespa.relay.server

import com.vitorpamplona.negentropy.Negentropy
import com.vitorpamplona.negentropy.storage.StorageVector
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.crypto.KeyPair
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip42RelayAuth.RelayAuthEvent
import com.vitorpamplona.quartz.utils.Hex
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.contentOrNull
import kotlinx.serialization.json.int
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.WebSocket
import java.security.MessageDigest
import java.util.Base64
import java.util.concurrent.CompletionStage
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test
import kotlin.test.assertTrue

/**
 * THE USER-FACING CONTRACT, CHECKED ON THE WIRE — against a running relay, by a client.
 *
 * Every other test here drives the relay in-process over an in-memory index, which cannot rank
 * or gate: so the upgrade that shipped a trust-gated COUNT answering UNGATED (vespa-eventstore
 * before #148) passed all of them. This one runs from the outside, against whatever
 * `-DitRelay=ws://…` points at, and checks what README.md and the NIP-11 document promise a
 * client, one clause per check:
 *
 *  - NIP-11: the NIPs and the limits the document advertises, and that they are ENFORCED
 *    (`default_limit`, `max_limit`, `max_filters`, `max_subid_length`).
 *  - "Every read says whose eyes it is read through": REQ, COUNT and NEG-OPEN are refused with
 *    `auth-required:` until a filter names an `observer:` or waives with `include:spam`, or the
 *    connection signs a NIP-42 AUTH; one undeclared filter poisons the request.
 *  - The trust gate, as a MATRIX against an oracle: NIP-01 shapes x lens modes (token, AUTH,
 *    both, a lens-less observer, `include:spam`, every `filter:rank` form) x `sort:` x search
 *    shapes. The REQ serves exactly what the oracle admits (in NIP-01 order where promised),
 *    and COUNT is exactly the size of the match set (NIP-45).
 *  - "A search answers with what its hits are about": list and label subjects splice in,
 *    bounded by the filter's kinds and by the observer's enrolment.
 *  - Writes: NIP-01 OK semantics, replaceable/addressable supersession, NIP-09, NIP-40, NIP-62
 *    (scoped to this relay's own url), ephemeral events, live delivery.
 *  - NIP-77 reconciles exactly the declared filter's set; NIP-86 answers admins only, and a
 *    ban it records is enforced on the next publish.
 *
 * THIS TEST WRITES. It publishes its own fixture under fresh keys (every read is scoped to them,
 * so reruns against the same store do not collide) — so point it only at a DISPOSABLE relay,
 * never at staging or production. The relay needs the operator settings it is started with:
 *
 *   docker run -d -p 8080:8080 -p 19071:19071 vespaengine/vespa
 *   RELAY_URL=ws://localhost:7777 VESPA_URL=http://localhost:8080 DEFAULT_LIMIT=25 MAX_LIMIT=40 \
 *     RELAY_ADMIN_PUBKEYS=npub10xlxvlhemja6c4dqv22uapctqupfhlxm9h8z3k2e72q4k9hcz7vqpkge6d \
 *     ./gradlew :relay:run
 *   ./gradlew :relay:test --tests '*RelayContractIT*' -DitRelay=ws://localhost:7777 --rerun -i
 *
 * The admin npub is the key whose secret is 1 ([ADMIN_SECRET]) — a key nobody should ever run a
 * real relay with, which is the point. The small limits keep the enforcement checks cheap: the
 * fixture is sized so every matrix read fits under `max_limit`.
 *
 * Failures are collected and printed as one table rather than thrown at the first: the point is
 * the whole contract's state.
 *
 * MEASURED 2026-09-28 (Vespa 8.754.14, 8,189 checks), one relay per pin set:
 *
 *  - store 9522ee0768, quartz fcd76d2075: 1,741 failing — 1,735 matrix COUNTs over the gated page
 *    (vespa-eventstore #148), plus the five below.
 *  - store ff54c9beb1, quartz fcd76d2075: 5 failing — gaps the contract had before either bump:
 *    NIP-77 capped at `default_limit` (quartz's LimitsPolicy stamped the REQ page limit on
 *    NEG-OPEN); an `observer:` NEG-OPEN reconciling nothing (the store's snapshot never resolved
 *    the lens); live delivery not trust-gated; NIP-62 honouring only the trailing-slash url
 *    (quartz compared the tag as a string).
 *  - store dfd8226ad0, quartz cba4a2d990: 0 failing — vitorpamplona/amethyst#4243 and
 *    NosFabrica/vespa-eventstore#155, with ObserverBackend holding live deliveries to the store's
 *    LiveGate.
 *  - store 6b9a1a8d3b, quartz bf2fefc621 (the merge of #4243): 0 of 8,191 failing — the two
 *    checks added since, a gated feed beside an ungated search filter, are the case #155's audit
 *    found: the search filter vouched for below-floor events the gated one was there to drop.
 *  - store 14d8c7eeed, quartz 68268da413 (2026-10-07, Vespa 8.763.13): 0 of 8,191 failing.
 *  - store a995519bef, quartz 68268da413 (2026-10-09, Vespa 8.763.13): 0 of 8,191 failing.
 */
class RelayContractIT {
    private val relay = System.getProperty("itRelay")

    @Test
    fun `the relay keeps every promise its README and NIP-11 make`() {
        if (relay == null) {
            println("CONTRACT-IT skipped — needs -DitRelay=ws://… of a DISPOSABLE relay (see the KDoc)")
            return
        }
        val http = relay.replaceFirst("ws", "http")
        val info = nip11(http)
        val limits = info["limitation"]!!.jsonObject
        val defaultLimit = limits["default_limit"]!!.jsonPrimitive.int
        val maxLimit = limits["max_limit"]!!.jsonPrimitive.int
        val maxFilters = limits["max_filters"]!!.jsonPrimitive.int
        val maxSubid = limits["max_subid_length"]!!.jsonPrimitive.int

        val fx = Fixture()
        val report = Report()

        Wire.open(relay).use { anon ->
            fx.publishAll(anon, report)
            fx.awaitLens(anon)

            section(report, "nip-11") { nip11Checks(info, report) }
            section(report, "limits") { limitChecks(anon, fx, report, defaultLimit, maxLimit, maxFilters, maxSubid) }
            section(report, "read lens") { lensPolicyChecks(anon, fx, report) }
            Wire.open(relay).use { authO ->
                authO.authenticate(fx.observer, relay)
                Wire.open(relay).use { authS ->
                    authS.authenticate(fx.stranger, relay)
                    section(report, "matrix") { matrix(anon, authO, authS, fx, report, maxLimit) }
                }
                section(report, "expansion") { expansionChecks(anon, fx, report) }
                section(report, "nip-42") { authChecks(fx, report) }
                section(report, "nip-77") { negentropyChecks(anon, fx, report) }
                section(report, "nip-86") { managementChecks(anon, http, report) }
                // Last among the fixture's readers: it publishes new notes by fixture authors.
                section(report, "live") { liveChecks(anon, authO, fx, report) }
            }
            section(report, "writes") { writeChecks(anon, fx, report) }
        }

        println("relay contract @ $relay: ${report.checks} checks, ${report.failures.size} failing")
        report.failures.forEach { println("  FAIL $it") }
        assertTrue(report.failures.isEmpty(), "${report.failures.size} of ${report.checks} contract checks failed:\n" + report.failures.joinToString("\n"))
    }

    // ---- NIP-11 -------------------------------------------------------------------------------

    private fun nip11(http: String): JsonObject {
        val response =
            HttpClient.newHttpClient().send(
                HttpRequest
                    .newBuilder(URI(http))
                    .header("Accept", "application/nostr+json")
                    .GET()
                    .build(),
                HttpResponse.BodyHandlers.ofString(),
            )
        return Json.parseToJsonElement(response.body()).jsonObject
    }

    private fun nip11Checks(
        info: JsonObject,
        report: Report,
    ) {
        val nips =
            info["supported_nips"]
                ?.jsonArray
                ?.map { it.jsonPrimitive.int }
                .orEmpty()
                .toSet()
        report.check("NIP-11 lists the README's NIPs", nips.containsAll(listOf(1, 9, 11, 40, 42, 45, 50, 62, 77, 86))) { "supported_nips=$nips" }
        report.check(
            "NIP-11 auth_required is false (observer:/include:spam need no signature)",
            info["limitation"]
                ?.jsonObject
                ?.get("auth_required")
                ?.jsonPrimitive
                ?.contentOrNull == "false",
        )
        val ext = info["nip50"]?.jsonArray?.map { it.jsonPrimitive.content }.orEmpty()
        report.check("NIP-11 nip50 advertises the extensions the README documents", ext.containsAll(listOf("ext observer", "ext include:spam", "ext sort", "ext filter:rank"))) { "nip50=$ext" }
    }

    // ---- NIP-11 limits, enforced ----------------------------------------------------------------

    private fun limitChecks(
        wire: Wire,
        fx: Fixture,
        report: Report,
        defaultLimit: Int,
        maxLimit: Int,
        maxFilters: Int,
        maxSubid: Int,
    ) {
        val notes = fx.notes.sortedWith(NEWEST_FIRST)
        report.check("fixture outgrows default_limit ($defaultLimit) so the default is observable", notes.size > defaultLimit) { "${notes.size} notes" }

        val unlimited = wire.req(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, search = "include:spam")))
        report.check("a REQ with no limit is served default_limit ($defaultLimit), newest first", unlimited.ids == notes.take(defaultLimit).map { it.id }) { "served ${unlimited.ids.size}" }
        val count = wire.count(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, search = "include:spam")))
        report.check("COUNT is the match set, not default_limit", count.count == notes.size) { "COUNT ${count.count}, matches ${notes.size}" }

        val everything = fx.published.sortedWith(NEWEST_FIRST)
        report.check("fixture outgrows max_limit ($maxLimit) so the clamp is observable", everything.size > maxLimit) { "${everything.size} events" }
        val over = wire.req(listOf(Filter(authors = fx.publishedKeys, limit = maxLimit * 10, search = "include:spam")))
        report.check("a REQ asking past max_limit is clamped to max_limit", over.ids == everything.take(maxLimit).map { it.id }) { "served ${over.ids.size}, want $maxLimit" }
        val overCount = wire.count(listOf(Filter(authors = fx.publishedKeys, limit = maxLimit * 10, search = "include:spam")))
        // A COUNT honours its filter's limit (STORE-C01), and the relay clamps that limit to
        // max_limit before the store sees it — so a count past max_limit reads max_limit. Pinned
        // as found: a client sizing a slice leaves `limit` out, and then counts it whole (above).
        report.check("COUNT with a limit past max_limit reads max_limit (the limit is clamped first)", overCount.count == maxLimit) { "COUNT ${overCount.count}, max_limit $maxLimit, matches ${everything.size}" }

        val tooMany = (0..maxFilters).map { Filter(kinds = listOf(1), authors = listOf(fx.authors[it % fx.authors.size].pubKey), search = "include:spam") }
        val refused = wire.req(tooMany)
        report.check("${maxFilters + 1} filters (max_filters $maxFilters) are refused", refused.closed != null || refused.notice != null) { "served ${refused.ids.size}, closed=${refused.closed}" }
        val justRight = wire.req(tooMany.take(maxFilters))
        report.check("$maxFilters filters are answered", justRight.closed == null && justRight.eose) { "closed=${justRight.closed}" }

        val longSub = "s".repeat(maxSubid + 1)
        val subRefused = wire.req(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, search = "include:spam")), subId = longSub)
        report.check("a sub id past max_subid_length ($maxSubid) is refused", !subRefused.eose || subRefused.notice != null || subRefused.closed != null) { "eose=${subRefused.eose}" }
    }

    // ---- "Every read says whose eyes it is read through" ----------------------------------------

    private fun lensPolicyChecks(
        wire: Wire,
        fx: Fixture,
        report: Report,
    ) {
        val plain = Filter(kinds = listOf(1), authors = fx.authorKeys)
        val bare = wire.req(listOf(plain))
        report.check("an undeclared REQ is CLOSED auth-required:", bare.closed?.startsWith("auth-required:") == true) { "closed=${bare.closed} served=${bare.ids.size}" }
        report.check("…and the refusal names both ways past it", bare.closed?.let { "observer:" in it && "include:spam" in it } == true) { "${bare.closed}" }
        val bareCount = wire.count(listOf(plain))
        report.check("an undeclared COUNT is CLOSED auth-required:", bareCount.closed?.startsWith("auth-required:") == true) { "count=${bareCount.count} closed=${bareCount.closed}" }
        val searchOnly = wire.req(listOf(plain.copy(search = "pizza")))
        report.check("a search that names no observer is not a declaration", searchOnly.closed?.startsWith("auth-required:") == true)
        val floorOnly = wire.req(listOf(plain.copy(search = "filter:rank:gte:50")))
        report.check("filter:rank alone is not a declaration", floorOnly.closed?.startsWith("auth-required:") == true)
        val sortOnly = wire.req(listOf(plain.copy(search = "sort:text")))
        report.check("sort:text alone is not a declaration", sortOnly.closed?.startsWith("auth-required:") == true)
        val poisoned = wire.req(listOf(plain.copy(search = "include:spam"), Filter(kinds = listOf(7), authors = fx.authorKeys)))
        report.check("one undeclared filter poisons the REQ", poisoned.closed?.startsWith("auth-required:") == true)
        val npub = wire.req(listOf(plain.copy(search = "observer:npub1qqqqqqqqqqqqqqqqqqqqqqqqqqqq")))
        report.check("an observer that is not 64-hex is not a lens", npub.closed?.startsWith("auth-required:") == true)
        val waived = wire.req(listOf(plain.copy(search = "include:spam", limit = 40)))
        report.check("include:spam is answered, whole and ungated", waived.eose && waived.ids.toSet() == fx.notes.map { it.id }.toSet()) { "served ${waived.ids.size} of ${fx.notes.size}" }
        val named = wire.req(listOf(plain.copy(search = "observer:${fx.observer.pubKey}", limit = 40)))
        report.check("observer:<hex> is answered", named.eose && named.closed == null)
        val negBare = wire.negOpen(Filter(kinds = listOf(1), authors = fx.authorKeys))
        report.check("an undeclared NEG-OPEN is NEG-ERR auth-required:", negBare.error?.contains("auth-required:") == true) { "error=${negBare.error}" }
    }

    // ---- the trust gate, as a matrix --------------------------------------------------------------

    private fun matrix(
        anon: Wire,
        authO: Wire,
        authS: Wire,
        fx: Fixture,
        report: Report,
        maxLimit: Int,
    ) {
        val lenses =
            listOf(
                Lens("observer", anon, "observer:${fx.observer.pubKey}", fx.observer.pubKey),
                Lens("AUTH observer", authO, null, fx.observer.pubKey),
                Lens("observer include:spam", anon, "observer:${fx.observer.pubKey} include:spam", fx.observer.pubKey),
                Lens("include:spam", anon, "include:spam", null),
                Lens("observer gte:50", anon, "observer:${fx.observer.pubKey} filter:rank:gte:50", fx.observer.pubKey),
                Lens("observer gte:90", anon, "observer:${fx.observer.pubKey} filter:rank:gte:90", fx.observer.pubKey),
                Lens("observer gt:49", anon, "observer:${fx.observer.pubKey} filter:rank:gt:49", fx.observer.pubKey),
                Lens("observer gte:0", anon, "observer:${fx.observer.pubKey} filter:rank:gte:0", fx.observer.pubKey),
                Lens("AUTH observer gte:50", authO, "filter:rank:gte:50", fx.observer.pubKey),
                Lens("AUTH observer include:spam", authO, "include:spam", fx.observer.pubKey),
                Lens("observer gte:50 include:spam", anon, "observer:${fx.observer.pubKey} filter:rank:gte:50 include:spam", fx.observer.pubKey),
                Lens("lens-less observer", anon, "observer:${fx.stranger.pubKey}", fx.stranger.pubKey),
                Lens("lens-less observer include:spam", anon, "observer:${fx.stranger.pubKey} include:spam", fx.stranger.pubKey),
                Lens("token beats AUTH", authS, "observer:${fx.observer.pubKey} filter:rank:gte:50", fx.observer.pubKey),
            )
        val sorts = listOf(null, "sort:rank", "sort:rank:asc", "sort:followers", "sort:text", "sort:recent")
        val texts = listOf(null, "pizza", "bitcoin", "\"pizza party\"", "-bitcoin", "pizza -bitcoin")
        for (lens in lenses) {
            for (sort in sorts) {
                for (text in texts) {
                    for ((name, base) in fx.bases) {
                        val filter = base.copy(limit = maxLimit, search = listOfNotNull(text, lens.tokens, sort).joinToString(" ").ifBlank { null })
                        val case = "[${lens.name}] [${sort ?: "-"}] [${text ?: "-"}] $name"
                        matrixCase(lens, listOf(filter), fx, report, case, sort)
                    }
                }
            }
            // Multi-filter: the page is one union, deduplicated, and COUNT is its size.
            val multi =
                listOf(
                    Filter(kinds = listOf(1), authors = listOf(fx.authors[0].pubKey), limit = maxLimit, search = lens.tokens),
                    Filter(kinds = listOf(1), authors = fx.authorKeys, tags = mapOf("t" to listOf("food")), limit = maxLimit, search = lens.tokens),
                )
            matrixCase(lens, multi, fx, report, "[${lens.name}] multi: A1 notes + #t food (overlap)", null)
        }
    }

    private fun matrixCase(
        lens: Lens,
        filters: List<Filter>,
        fx: Fixture,
        report: Report,
        case: String,
        sort: String?,
    ) {
        val expected = fx.scopedAll.filter { e -> filters.any { f -> fx.admits(f, e, lens.observer, sort) } }
        val served = lens.wire.req(filters)
        val counted = lens.wire.count(filters)
        val problems = ArrayList<String>()
        if (served.closed != null) problems += "REQ CLOSED ${served.closed}"
        if (counted.closed != null) problems += "COUNT CLOSED ${counted.closed}"
        val want = expected.map { it.id }.toSet()
        val got = served.ids.toSet()
        if (got != want) {
            problems += "served ${got.size}, oracle ${want.size}" +
                (want - got).takeIf { it.isNotEmpty() }?.let { " | missing ${fx.describe(it)}" }.orEmpty() +
                (got - want).takeIf { it.isNotEmpty() }?.let { " | extra ${fx.describe(it)}" }.orEmpty()
        }
        if (served.ids.size != got.size) problems += "served duplicates"
        if (counted.count != want.size) problems += "COUNT ${counted.count} != oracle ${want.size}"
        val termless = filters.all { f -> fx.parse(f.search).let { it.terms.isEmpty() && it.phrases.isEmpty() } }
        val chronological = sort == "sort:recent" || (termless && sort == null)
        if (chronological && problems.isEmpty() && served.ids != expected.sortedWith(NEWEST_FIRST).map { it.id }) problems += "not in NIP-01 order"
        if (termless && lens.observer == fx.observer.pubKey && problems.isEmpty()) {
            val ranks = served.events.map { fx.rank[it.pubKey] ?: 0 }
            when (sort) {
                "sort:rank" -> {
                    if (ranks.zipWithNext().any { (a, b) -> a < b }) problems += "sort:rank not trust-descending"
                }

                "sort:rank:asc" -> {
                    if (ranks.zipWithNext().any { (a, b) -> a > b }) problems += "sort:rank:asc not trust-ascending"
                }

                "sort:followers" -> {
                    if (served.events
                            .map { fx.followers[it.pubKey] ?: 0 }
                            .zipWithNext()
                            .any { (a, b) -> a < b }
                    ) {
                        problems += "sort:followers not follower-descending"
                    }
                }
            }
        }
        report.check(case, problems.isEmpty()) { filters.joinToString(" + ") { it.toJson() } + "\n    " + problems.joinToString("\n    ") }
    }

    private class Lens(
        val name: String,
        val wire: Wire,
        val tokens: String?,
        /** The observer the read resolves to: the token's, else the connection's. */
        val observer: String?,
    )

    // ---- live delivery -------------------------------------------------------------------------

    private fun liveChecks(
        anon: Wire,
        authO: Wire,
        fx: Fixture,
        report: Report,
    ) {
        val now = nowSecs()
        val open = anon.subscribe(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, since = now - 5, search = "include:spam")))
        val gated = authO.subscribe(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, since = now - 5)))
        val trusted = fx.authors[0].sign<Event>(now, 1, emptyArray(), "live from a trusted author")
        val untrusted = fx.authors[5].sign<Event>(now, 1, emptyArray(), "live from a below-floor author")
        anon.publish(trusted)
        anon.publish(untrusted)
        val openGot = anon.drain(open, 3_000)
        val gatedGot = authO.drain(gated, 3_000)
        report.check("a live subscription receives matching events", trusted.id in openGot && untrusted.id in openGot) { "got ${openGot.size}" }
        report.check("an AUTH'd live feed receives a trusted author's event", trusted.id in gatedGot) { "got ${gatedGot.size}" }
        report.check("an AUTH'd live feed is trusted-only (README: plain filters become trusted-only feeds)", untrusted.id !in gatedGot) { "the below-floor author's live note was delivered" }
        anon.close(open)
        authO.close(gated)
        fx.extra += listOf(trusted, untrusted)

        // Many filters, one gated: an ungated SEARCH beside a gated plain filter vouches only
        // for what its words match — quartz's in-memory match ignores `search`, and used to let
        // the search filter admit a below-floor note the plain filter was there to drop.
        val later = nowSecs()
        val mixed =
            authO.subscribe(
                listOf(
                    Filter(kinds = listOf(1), authors = fx.authorKeys, since = later - 5, search = "quokka sort:text"),
                    Filter(kinds = listOf(1), authors = fx.authorKeys, since = later - 5),
                ),
            )
        val offTopic = fx.authors[5].sign<Event>(later, 1, emptyArray(), "a below-floor author says hello")
        val onTopic = fx.authors[5].sign<Event>(later, 1, emptyArray(), "a below-floor quokka sighting")
        anon.publish(offTopic)
        anon.publish(onTopic)
        val mixedGot = authO.drain(mixed, 3_000)
        report.check("a gated sibling filter is not bypassed by an ungated search filter", offTopic.id !in mixedGot) { "the below-floor note matched only the gated filter and was delivered" }
        report.check("the ungated search filter still streams what its words match", onTopic.id in mixedGot) { "got ${mixedGot.size}" }
        authO.close(mixed)
        fx.extra += listOf(offTopic, onTopic)
    }

    // ---- "A search answers with what its hits are about" ------------------------------------------

    private fun expansionChecks(
        wire: Wire,
        fx: Fixture,
        report: Report,
    ) {
        val o = fx.observer.pubKey
        val list = fx.trustedList
        val members = fx.members.map { it.second.id }.toSet()
        val bothKinds = wire.req(listOf(Filter(kinds = listOf(0, 30392), authors = listOf(fx.curator.pubKey) + fx.memberKeys, search = "${fx.listWord} observer:$o")))
        report.check("a list hit serves the list and its members' profiles", list.id in bothKinds.ids && members.all { it in bothKinds.ids }) { "served ${fx.describe(bothKinds.ids.toSet())}" }
        val trustedAt = bothKinds.ids.indexOf(fx.members[0].second.id)
        val doubtedAt = bothKinds.ids.indexOf(fx.members[1].second.id)
        report.check("a spliced member its publisher doubts sinks below the one it trusts", trustedAt >= 0 && doubtedAt >= 0 && trustedAt < doubtedAt) { "trusted at $trustedAt, doubted at $doubtedAt" }
        val count = wire.count(listOf(Filter(kinds = listOf(0, 30392), authors = listOf(fx.curator.pubKey) + fx.memberKeys, search = "${fx.listWord} observer:$o")))
        report.check("COUNT of a splicing search counts the MATCHES, not the spliced rows (store contract)", count.count == 1) { "COUNT ${count.count}" }
        val listOnly = wire.req(listOf(Filter(kinds = listOf(30392), authors = listOf(fx.curator.pubKey) + fx.memberKeys, search = "${fx.listWord} observer:$o")))
        report.check("a subject must match the filter's own kinds", listOnly.ids == listOf(list.id)) { "served ${fx.describe(listOnly.ids.toSet())}" }
        val anonymous = wire.req(listOf(Filter(kinds = listOf(0, 30392), authors = listOf(fx.curator.pubKey) + fx.memberKeys, search = "${fx.listWord} include:spam")))
        report.check("an anonymous include:spam read gets the list but no expansion", anonymous.ids == listOf(list.id)) { "served ${fx.describe(anonymous.ids.toSet())}" }
        val notEnrolled = wire.req(listOf(Filter(kinds = listOf(0, 30392), authors = listOf(fx.curator.pubKey) + fx.memberKeys, search = "${fx.listWord} observer:${fx.stranger.pubKey} include:spam")))
        report.check("an observer who never enrolled the curator gets no expansion", notEnrolled.ids == listOf(list.id)) { "served ${fx.describe(notEnrolled.ids.toSet())}" }
        val label = wire.req(listOf(Filter(kinds = listOf(0, 1985), authors = listOf(fx.labeler.pubKey) + fx.memberKeys, search = "${fx.labelWord} observer:$o")))
        report.check("a label hit serves its p subject's profile (labels need no enrolment)", fx.label.id in label.ids && fx.members[0].second.id in label.ids) { "served ${fx.describe(label.ids.toSet())}" }
    }

    // ---- writes --------------------------------------------------------------------------------

    private fun writeChecks(
        wire: Wire,
        fx: Fixture,
        report: Report,
    ) {
        val w = NostrSignerSync()
        val scope = listOf(w.pubKey)
        val now = nowSecs()

        fun served(vararg kinds: Int) = wire.req(listOf(Filter(kinds = kinds.toList(), authors = scope, search = "include:spam"))).ids

        val note = w.sign<Event>(now - 100, 1, emptyArray(), "a note")
        report.check("a valid EVENT is OK true", wire.publish(note).accepted)
        val dup = wire.publish(note)
        report.check("a duplicate is OK true, duplicate:", dup.accepted && dup.message.startsWith("duplicate:")) { "OK ${dup.accepted} '${dup.message}'" }
        val forged = Event(note.id, note.pubKey, note.createdAt, note.kind, note.tags, "tampered", note.sig)
        report.check("a forged event is OK false", !wire.publish(forged).accepted)

        val v1 = w.sign<Event>(now - 50, 0, emptyArray(), """{"name":"v1"}""")
        val v2 = w.sign<Event>(now - 40, 0, emptyArray(), """{"name":"v2"}""")
        wire.publish(v1)
        wire.publish(v2)
        report.check("a replaceable serves only its newest version", served(0) == listOf(v2.id)) { "served ${served(0)}" }
        val stale = wire.publish(v1)
        report.check("republishing a superseded replaceable is not accepted back", served(0) == listOf(v2.id)) { "OK ${stale.accepted} '${stale.message}'" }
        val a1 = w.sign<Event>(now - 50, 30023, arrayOf(arrayOf("d", "post")), "draft")
        val a2 = w.sign<Event>(now - 40, 30023, arrayOf(arrayOf("d", "post")), "final")
        val other = w.sign<Event>(now - 45, 30023, arrayOf(arrayOf("d", "other")), "other")
        listOf(a1, a2, other).forEach { wire.publish(it) }
        report.check("an addressable serves the newest per d-tag", served(30023).toSet() == setOf(a2.id, other.id)) { "served ${served(30023)}" }

        val doomed = w.sign<Event>(now - 30, 1, emptyArray(), "delete me")
        wire.publish(doomed)
        val deletion = w.sign<Event>(now - 20, 5, arrayOf(arrayOf("e", doomed.id), arrayOf("k", "1")), "")
        report.check("a NIP-09 deletion is accepted", wire.publish(deletion).accepted)
        report.check("NIP-09: the deleted event is no longer served", doomed.id !in served(1))
        val back = wire.publish(doomed)
        report.check("NIP-09: a deleted event cannot be republished", !back.accepted && doomed.id !in served(1)) { "OK ${back.accepted} '${back.message}'" }
        val intruder = NostrSignerSync()
        val foreign = intruder.sign<Event>(now - 10, 5, arrayOf(arrayOf("e", note.id)), "")
        wire.publish(foreign)
        report.check("NIP-09: another author's deletion deletes nothing", note.id in served(1))

        val expired = w.sign<Event>(now - 5, 1, arrayOf(arrayOf("expiration", (now - 1).toString())), "already expired")
        wire.publish(expired)
        report.check("NIP-40: an already-expired event is never served", expired.id !in served(1))
        val soon = w.sign<Event>(now - 4, 1, arrayOf(arrayOf("expiration", (now + 3).toString())), "expires soon")
        wire.publish(soon)
        report.check("NIP-40: an unexpired event is served", soon.id in served(1))
        Thread.sleep(5_000)
        report.check("NIP-40: …and stops being served once it expires", soon.id !in served(1))

        val eph = w.sign<Event>(nowSecs(), 20001, emptyArray(), "ephemeral")
        val live = wire.subscribe(listOf(Filter(kinds = listOf(20001), authors = scope, search = "include:spam")))
        wire.publish(eph)
        report.check("an ephemeral event is delivered live", eph.id in wire.drain(live, 2_000))
        wire.close(live)
        report.check("an ephemeral event is never stored", served(20001).isEmpty())

        val v = NostrSignerSync()
        val older = v.sign<Event>(nowSecs() - 60, 1, emptyArray(), "before vanishing")
        wire.publish(older)
        val elsewhere = v.sign<Event>(nowSecs() - 30, 62, arrayOf(arrayOf("relay", "wss://some.other.relay/")), "")
        wire.publish(elsewhere)
        val vScope = listOf(v.pubKey)
        report.check("NIP-62: a vanish scoped to another relay changes nothing here", older.id in wire.req(listOf(Filter(kinds = listOf(1), authors = vScope, search = "include:spam"))).ids)
        val vanish = v.sign<Event>(nowSecs(), 62, arrayOf(arrayOf("relay", relay)), "")
        report.check("NIP-62: a vanish naming this relay is accepted", wire.publish(vanish).accepted)
        report.check("NIP-62: the author's older events are gone", older.id !in wire.req(listOf(Filter(kinds = listOf(1), authors = vScope, search = "include:spam"))).ids)
        val retry = wire.publish(older)
        report.check("NIP-62: an older event cannot be republished", !retry.accepted) { "OK ${retry.accepted} '${retry.message}'" }
        // The same vanish naming the url in its NORMALIZED form (trailing slash): the mechanism
        // itself, separated from how the tag's url is compared.
        val vn = NostrSignerSync()
        val older2 = vn.sign<Event>(nowSecs() - 60, 1, emptyArray(), "before vanishing")
        wire.publish(older2)
        wire.publish(vn.sign<Event>(nowSecs(), 62, arrayOf(arrayOf("relay", RelayUrlNormalizer.normalize(relay).url)), ""))
        report.check("NIP-62: a vanish naming the normalized url removes the author's events", older2.id !in wire.req(listOf(Filter(kinds = listOf(1), authors = listOf(vn.pubKey), search = "include:spam"))).ids)
    }

    // ---- NIP-42 --------------------------------------------------------------------------------

    private fun authChecks(
        fx: Fixture,
        report: Report,
    ) {
        Wire.open(relay).use { wire ->
            val challenge = wire.challenge()
            val wrong = fx.observer.sign(RelayAuthEvent.build(RelayUrlNormalizer.normalize("wss://not.this.relay/"), challenge))
            report.check("AUTH for another relay's url is refused", !wire.auth(wrong).accepted)
            val badChallenge = fx.observer.sign(RelayAuthEvent.build(RelayUrlNormalizer.normalize(relay), "not-the-challenge"))
            report.check("AUTH with the wrong challenge is refused", !wire.auth(badChallenge).accepted)
            val right = fx.observer.sign(RelayAuthEvent.build(RelayUrlNormalizer.normalize(relay), challenge))
            report.check("AUTH for this relay is accepted", wire.auth(right).accepted)
            val after = wire.req(listOf(Filter(kinds = listOf(1), authors = fx.authorKeys, limit = 40)))
            report.check(
                "after AUTH an undeclared REQ is answered through the connection's lens",
                after.closed == null && after.ids.toSet() ==
                    fx.notes
                        .filter { (fx.rank[it.pubKey] ?: 0) >= 2 }
                        .map { it.id }
                        .toSet(),
            ) { "closed=${after.closed} served=${after.ids.size}" }
        }
    }

    // ---- NIP-77 --------------------------------------------------------------------------------

    private fun negentropyChecks(
        wire: Wire,
        fx: Fixture,
        report: Report,
    ) {
        val filter = Filter(kinds = listOf(1), authors = fx.authorKeys, search = "include:spam")
        val set = wire.negOpen(filter)
        report.check("a declared NEG-OPEN reconciles", set.error == null) { "error=${set.error}" }
        report.check("NIP-77 needIds are exactly the declared filter's set", set.need == fx.notes.map { it.id }.toSet()) { "need ${set.need.size}, set ${fx.notes.size}" }
        val lensed = wire.negOpen(filter.copy(search = "observer:${fx.observer.pubKey}"))
        report.check("an observer-declared NEG-OPEN is admitted", lensed.error == null) { "error=${lensed.error}" }
        val gated =
            fx.notes
                .filter { (fx.rank[it.pubKey] ?: 0) >= 2 }
                .map { it.id }
                .toSet()
        report.check(
            "an observer-declared NEG-OPEN reconciles the same set its REQ serves (${gated.size} gated / ${fx.notes.size} ungated)",
            lensed.need == gated,
        ) { "need ${lensed.need.size}" }
    }

    // ---- NIP-86 --------------------------------------------------------------------------------

    private fun managementChecks(
        wire: Wire,
        http: String,
        report: Report,
    ) {
        val client = HttpClient.newHttpClient()
        val admin = NostrSignerSync(KeyPair(privKey = ADMIN_SECRET))

        fun rpc(
            signer: NostrSignerSync?,
            method: String,
            params: String = "[]",
        ): HttpResponse<String> {
            val body = """{"method":"$method","params":$params}"""
            val request = HttpRequest.newBuilder(URI(http)).header("Content-Type", "application/nostr+json+rpc").POST(HttpRequest.BodyPublishers.ofString(body))
            if (signer != null) {
                val payload = Hex.encode(MessageDigest.getInstance("SHA-256").digest(body.toByteArray()))
                val token = signer.sign<Event>(nowSecs(), 27235, arrayOf(arrayOf("u", http), arrayOf("method", "POST"), arrayOf("payload", payload)), "")
                request.header("Authorization", "Nostr " + Base64.getEncoder().encodeToString(token.toJson().toByteArray()))
            }
            return client.send(request.build(), HttpResponse.BodyHandlers.ofString())
        }

        report.check("NIP-86 without NIP-98 is 401", rpc(null, "supportedmethods").statusCode() == 401)
        report.check("NIP-86 from a non-admin is 403", rpc(NostrSignerSync(), "supportedmethods").statusCode() == 403)
        val methods = rpc(admin, "supportedmethods")
        report.check("NIP-86 supportedmethods answers the admin", methods.statusCode() == 200 && "banpubkey" in methods.body()) { "${methods.statusCode()} ${methods.body().take(200)}" }
        val banned = NostrSignerSync()
        val ban = rpc(admin, "banpubkey", """["${banned.pubKey}","contract test"]""")
        report.check("NIP-86 banpubkey succeeds", ban.statusCode() == 200 && "error" !in Json.parseToJsonElement(ban.body()).jsonObject.filterValues { it !is kotlinx.serialization.json.JsonNull }) { ban.body() }
        report.check("NIP-86 listbannedpubkeys shows the ban", banned.pubKey in rpc(admin, "listbannedpubkeys").body())
        report.check("a banned pubkey's publish is refused", !wire.publish(banned.sign<Event>(nowSecs(), 1, emptyArray(), "banned")).accepted)
    }

    // ---- the fixture and its oracle --------------------------------------------------------------

    /**
     * Seven authors at fixed trust tiers under [observer]'s provider, four notes each (the texts
     * the matrix searches), three reactions; plus a curator's trusted list and a labeler's label
     * over two members, for the expansion checks. Fresh keys every run.
     */
    private class Fixture {
        val observer = NostrSignerSync()
        val stranger = NostrSignerSync()
        val provider = NostrSignerSync()
        val curator = NostrSignerSync()
        val labeler = NostrSignerSync()
        val authors = List(7) { NostrSignerSync() }
        val authorKeys = authors.map { it.pubKey }

        private val tiers = listOf(90 to 5, 60 to 50, 50 to 500, 10 to 5000, 2 to 1, 1 to 2)
        val rank: Map<String, Int> = authors.zip(tiers).associate { (a, t) -> a.pubKey to t.first } + mapOf(curator.pubKey to 80, labeler.pubKey to 40)
        val followers: Map<String, Int> = authors.zip(tiers).associate { (a, t) -> a.pubKey to t.second }

        private val base = nowSecs() - 10_000
        private val texts =
            listOf(
                "pizza party tonight" to arrayOf(arrayOf("t", "food")),
                "party pizza leftovers" to arrayOf(arrayOf("t", "food"), arrayOf("p", authors[0].pubKey)),
                "bitcoin price today" to arrayOf(arrayOf("t", "money")),
                "pizza with bitcoin" to arrayOf(arrayOf("t", "food"), arrayOf("t", "money")),
            )
        val notes: List<Event> = texts.flatMapIndexed { j, (text, tags) -> authors.mapIndexed { i, a -> a.sign<Event>(base + j * authors.size + i, 1, tags, text) } }
        val reactions: List<Event> =
            listOf(0, 3, 5).map { i ->
                val target = notes[i + 1]
                authors[i].sign<Event>(base + 100 + i, 7, arrayOf(arrayOf("e", target.id), arrayOf("p", target.pubKey)), "+")
            }
        val cards: List<Event> =
            rank.map { (subject, r) ->
                provider.sign<Event>(base - 100 + r, 30382, arrayOf(arrayOf("d", subject), arrayOf("rank", r.toString()), arrayOf("followers", (followers[subject] ?: 0).toString())), "")
            }
        val list10040: Event =
            observer.sign<Event>(
                base - 50,
                10040,
                arrayOf(
                    arrayOf("30382:rank", provider.pubKey, "wss://scores.test/"),
                    arrayOf("30382:followers", provider.pubKey, "wss://scores.test/"),
                    arrayOf("30392", curator.pubKey, "wss://lists.test/"),
                ),
                "",
            )

        // Expansion: words no note uses, so the matrix never meets these events.
        val listWord = "podcaster"
        val labelWord = "vegan"
        val members = List(2) { NostrSignerSync() }.mapIndexed { i, m -> m to m.sign<Event>(base + 200 + i, 0, emptyArray(), """{"name":"member${"one two".split(' ')[i]}"}""") }
        val memberKeys = members.map { it.first.pubKey }
        val trustedList: Event =
            curator.sign<Event>(
                base + 210,
                30392,
                arrayOf(
                    arrayOf("d", "podcasters"),
                    arrayOf("title", "Podcaster Trust List"),
                    arrayOf("p", memberKeys[0], "", "95"),
                    arrayOf("p", memberKeys[1], "", "5"),
                ),
                "",
            )
        val label: Event = labeler.sign<Event>(base + 220, 1985, arrayOf(arrayOf("L", "diet"), arrayOf("l", labelWord, "diet"), arrayOf("p", memberKeys[0])), "")

        /** Events the live checks add after the matrix ran; kept out of it. */
        val extra = ArrayList<Event>()

        /** The matrix's corpus: the authors' events, the provider's cards and the observer's 10040. */
        val allKeys = authorKeys + observer.pubKey + provider.pubKey
        val scopedAll: List<Event> get() = notes + reactions + cards + list10040

        val bases: List<Pair<String, Filter>> by lazy {
            val mid = notes.sortedBy { it.createdAt }[notes.size / 2].createdAt
            val early = notes.minOf { it.createdAt } + 5
            listOf(
                "everything" to Filter(authors = allKeys),
                "kind 1" to Filter(kinds = listOf(1), authors = authorKeys),
                "kinds 1,7" to Filter(kinds = listOf(1, 7), authors = authorKeys),
                "kind 7" to Filter(kinds = listOf(7), authors = authorKeys),
                "kind 30382" to Filter(kinds = listOf(30382), authors = listOf(provider.pubKey)),
                "authors A1,A4,A7" to Filter(authors = listOf(authorKeys[0], authorKeys[3], authorKeys[6])),
                "kind 1 by A3" to Filter(kinds = listOf(1), authors = listOf(authorKeys[2])),
                "kind 1 by A5,A6 (floor edge)" to Filter(kinds = listOf(1), authors = listOf(authorKeys[4], authorKeys[5])),
                "#t food" to Filter(authors = authorKeys, tags = mapOf("t" to listOf("food"))),
                "#t food|money" to Filter(authors = authorKeys, tags = mapOf("t" to listOf("food", "money"))),
                "&t food+money" to Filter(authors = authorKeys, tagsAll = mapOf("t" to listOf("food", "money"))),
                "#p A1" to Filter(authors = authorKeys, tags = mapOf("p" to listOf(authorKeys[0]))),
                "since mid" to Filter(authors = authorKeys, since = mid),
                "until mid" to Filter(authors = authorKeys, until = mid),
                "kind 1 window" to Filter(kinds = listOf(1), authors = authorKeys, since = early, until = mid),
                "ids x4" to Filter(ids = listOf(notes[0].id, notes[9].id, notes[20].id, reactions[1].id)),
            )
        }

        /** Everything [publishAll] sends, and every key that signed some of it. */
        val published: List<Event> get() = cards + notes + reactions + members.map { it.second } + trustedList + label + list10040
        val publishedKeys: List<String> get() = published.map { it.pubKey }.distinct()

        fun publishAll(
            wire: Wire,
            report: Report,
        ) {
            published.forEach { e ->
                val ok = wire.publish(e)
                report.check("fixture publish kind ${e.kind}", ok.accepted) { "'${ok.message}'" }
            }
        }

        /**
         * The 10040 is projected by a background worker: wait until the lens gates. Read off the
         * REQ, not a COUNT — the COUNT is one of the things under test, and a relay whose gated
         * COUNT is broken must still get as far as the matrix that says so.
         */
        fun awaitLens(wire: Wire) {
            val want = notes.filter { (rank[it.pubKey] ?: 0) >= 50 }.map { it.id }.toSet()
            val deadline = System.currentTimeMillis() + 60_000
            while (System.currentTimeMillis() < deadline) {
                if (wire.req(listOf(Filter(kinds = listOf(1), authors = authorKeys, limit = 40, search = "observer:${observer.pubKey} filter:rank:gte:50"))).ids.toSet() == want) return
                Thread.sleep(500)
            }
            error("the observer's lens never resolved (a gte:50 REQ never served its ${want.size} notes)")
        }

        class Parsed(
            val terms: List<String>,
            val phrases: List<List<String>>,
            val notTerms: List<String>,
            val floor: Int?,
            val includeSpam: Boolean,
        )

        fun parse(search: String?): Parsed {
            if (search == null) return Parsed(emptyList(), emptyList(), emptyList(), null, false)
            val phrases = Regex("\"([^\"]+)\"").findAll(search).map { tokens(it.groupValues[1]) }.toList()
            val words = search.replace(Regex("\"[^\"]*\""), " ").split(' ').filter { it.isNotBlank() }
            val floor = words.firstNotNullOfOrNull { w -> Regex("filter:rank:(gte|gt):(\\d+)").matchEntire(w)?.let { m -> m.groupValues[2].toInt() + if (m.groupValues[1] == "gt") 1 else 0 } }
            return Parsed(
                terms = words.filter { ':' !in it && !it.startsWith("-") },
                phrases = phrases,
                notTerms = words.filter { it.startsWith("-") && ':' !in it }.map { it.drop(1) },
                floor = floor,
                includeSpam = "include:spam" in words,
            )
        }

        private fun tokens(text: String): List<String> = text.lowercase().split(Regex("[^a-z0-9]+")).filter { it.isNotEmpty() }

        /**
         * The oracle: Quartz's own NIP-01 match, a token scan for text (only kind 1 carries any
         * in the matrix corpus), and the gate — a resolved observer's floor, the explicit one
         * surviving `include:spam`, none under `sort:text` (README: "ignoring the observer").
         */
        fun admits(
            f: Filter,
            e: Event,
            observerKey: String?,
            sort: String?,
        ): Boolean {
            if (!f.copy(search = null, limit = null).match(e)) return false
            val p = parse(f.search)
            if (p.terms.isNotEmpty() || p.phrases.isNotEmpty()) {
                if (e.kind != 1) return false
                val t = tokens(e.content)
                if (!p.terms.all { it in t }) return false
                if (!p.phrases.all { ph -> t.windowed(ph.size).any { it == ph } }) return false
            }
            if (p.notTerms.any { e.kind == 1 && it in tokens(e.content) }) return false
            if (sort == "sort:text") return true
            val floor = p.floor ?: if (p.includeSpam) null else 2
            val trust = if (observerKey == observer.pubKey) rank[e.pubKey] ?: 0 else 0
            return observerKey == null || floor == null || trust >= floor
        }

        private val labels: Map<String, String> by lazy {
            authorKeys.mapIndexed { i, k -> k to "A${i + 1}(${tiers.getOrNull(i)?.first ?: "-"})" }.toMap() +
                mapOf(provider.pubKey to "provider", observer.pubKey to "observer", curator.pubKey to "curator", labeler.pubKey to "labeler") +
                memberKeys.mapIndexed { i, k -> k to "M${i + 1}" }
        }
        private val byId: Map<String, Event> by lazy { (scopedAll + members.map { it.second } + trustedList + label + extra).associateBy { it.id } }

        fun describe(ids: Set<String>): String =
            ids
                .groupBy { id -> byId[id]?.let { "k${it.kind}/${labels[it.pubKey] ?: it.pubKey.take(6)}" } ?: "unknown ${id.take(8)}" }
                .entries
                .joinToString(", ") { (k, v) -> "$k x${v.size}" }
    }

    // ---- the client ------------------------------------------------------------------------------

    private class Ok(
        val accepted: Boolean,
        val message: String,
    )

    private class Page(
        val events: List<Event>,
        val eose: Boolean,
        val closed: String?,
        val notice: String?,
    ) {
        val ids: List<String> get() = events.map { it.id }
    }

    private class Counted(
        val count: Int?,
        val closed: String?,
    )

    private class Reconciled(
        val need: Set<String>,
        val error: String?,
    )

    /** One websocket, its frames routed by subscription id (OK by event id). */
    private class Wire private constructor(
        private val ws: WebSocket,
        private val routes: ConcurrentHashMap<String, LinkedBlockingQueue<JsonArray>>,
    ) : AutoCloseable {
        private fun queue(key: String) = routes.computeIfAbsent(key) { LinkedBlockingQueue() }

        private fun next(
            key: String,
            timeoutMs: Long = 20_000,
        ): JsonArray? = queue(key).poll(timeoutMs, TimeUnit.MILLISECONDS)

        fun send(frame: String) {
            ws.sendText(frame, true).get(10, TimeUnit.SECONDS)
        }

        fun req(
            filters: List<Filter>,
            subId: String = "r${SEQ.incrementAndGet()}",
        ): Page {
            send("""["REQ","$subId",${filters.joinToString(",") { it.toJson() }}]""")
            val events = ArrayList<Event>()
            while (true) {
                val frame = next(subId) ?: return Page(events, false, null, drainNotice())
                when (frame[0].jsonPrimitive.content) {
                    "EVENT" -> {
                        events += Event.fromJson(frame[2].toString())
                    }

                    "EOSE" -> {
                        send("""["CLOSE","$subId"]""")
                        return Page(events, true, null, null)
                    }

                    "CLOSED" -> {
                        return Page(events, false, frame[2].jsonPrimitive.content, null)
                    }
                }
            }
        }

        fun count(filters: List<Filter>): Counted {
            val subId = "c${SEQ.incrementAndGet()}"
            send("""["COUNT","$subId",${filters.joinToString(",") { it.toJson() }}]""")
            val frame = next(subId) ?: return Counted(null, "timeout")
            return when (frame[0].jsonPrimitive.content) {
                "COUNT" -> Counted(frame[2].jsonObject["count"]!!.jsonPrimitive.int, null)
                else -> Counted(null, frame.getOrNull(2)?.jsonPrimitive?.contentOrNull ?: frame.toString())
            }
        }

        fun subscribe(filters: List<Filter>): String {
            val subId = "l${SEQ.incrementAndGet()}"
            send("""["REQ","$subId",${filters.joinToString(",") { it.toJson() }}]""")
            while (true) {
                val frame = next(subId) ?: error("no EOSE on $subId")
                if (frame[0].jsonPrimitive.content == "EOSE") return subId
                if (frame[0].jsonPrimitive.content == "CLOSED") error("live sub refused: $frame")
            }
        }

        /** Live EVENT ids that arrive on [subId] within [waitMs]. */
        fun drain(
            subId: String,
            waitMs: Long,
        ): Set<String> {
            val ids = HashSet<String>()
            val deadline = System.currentTimeMillis() + waitMs
            while (true) {
                val left = deadline - System.currentTimeMillis()
                if (left <= 0) return ids
                val frame = next(subId, left) ?: return ids
                if (frame[0].jsonPrimitive.content == "EVENT") ids += frame[2].jsonObject["id"]!!.jsonPrimitive.content
            }
        }

        fun close(subId: String) = send("""["CLOSE","$subId"]""")

        fun publish(event: Event): Ok {
            send("""["EVENT",${event.toJson()}]""")
            val frame = next("OK:${event.id}") ?: return Ok(false, "timeout")
            return Ok(
                frame[2].jsonPrimitive.content == "true",
                frame
                    .getOrNull(3)
                    ?.jsonPrimitive
                    ?.contentOrNull
                    .orEmpty(),
            )
        }

        fun challenge(): String = next("AUTH")?.get(1)?.jsonPrimitive?.content ?: error("no AUTH challenge")

        fun auth(event: Event): Ok {
            send("""["AUTH",${event.toJson()}]""")
            val frame = next("OK:${event.id}") ?: return Ok(false, "timeout")
            return Ok(
                frame[2].jsonPrimitive.content == "true",
                frame
                    .getOrNull(3)
                    ?.jsonPrimitive
                    ?.contentOrNull
                    .orEmpty(),
            )
        }

        fun authenticate(
            signer: NostrSignerSync,
            relay: String,
        ) {
            val ok = auth(signer.sign(RelayAuthEvent.build(RelayUrlNormalizer.normalize(relay), challenge())))
            check(ok.accepted) { "AUTH refused: ${ok.message}" }
        }

        /** A full NIP-77 reconcile from an EMPTY local set: `need` is everything the relay has. */
        fun negOpen(filter: Filter): Reconciled {
            val subId = "n${SEQ.incrementAndGet()}"
            val neg = Negentropy(StorageVector().also { it.seal() }, 60_000)
            send("""["NEG-OPEN","$subId",${filter.toJson()},"${Hex.encode(neg.initiate())}"]""")
            val need = HashSet<String>()
            while (true) {
                val frame = next(subId) ?: return Reconciled(need, "timeout")
                when (frame[0].jsonPrimitive.content) {
                    "NEG-ERR" -> {
                        return Reconciled(need, frame[2].jsonPrimitive.content)
                    }

                    "NEG-MSG" -> {
                        val result = neg.reconcile(Hex.decode(frame[2].jsonPrimitive.content))
                        result.needIds.forEach { need += it.toHexString() }
                        val msg = result.msg
                        if (msg == null || msg.isEmpty()) {
                            send("""["NEG-CLOSE","$subId"]""")
                            return Reconciled(need, null)
                        }
                        send("""["NEG-MSG","$subId","${Hex.encode(msg)}"]""")
                    }
                }
            }
        }

        private fun drainNotice(): String? =
            routes["NOTICE"]
                ?.poll()
                ?.getOrNull(1)
                ?.jsonPrimitive
                ?.contentOrNull

        override fun close() {
            runCatching { ws.sendClose(WebSocket.NORMAL_CLOSURE, "done").get(5, TimeUnit.SECONDS) }
        }

        companion object {
            val SEQ = AtomicInteger()

            fun open(url: String): Wire {
                val routes = ConcurrentHashMap<String, LinkedBlockingQueue<JsonArray>>()
                val buffer = StringBuilder()
                val listener =
                    object : WebSocket.Listener {
                        override fun onText(
                            webSocket: WebSocket,
                            data: CharSequence,
                            last: Boolean,
                        ): CompletionStage<*>? {
                            buffer.append(data)
                            if (last) {
                                val frame = Json.parseToJsonElement(buffer.toString()).jsonArray
                                buffer.setLength(0)
                                val type = frame[0].jsonPrimitive.content
                                val key =
                                    when (type) {
                                        "OK" -> "OK:${frame[1].jsonPrimitive.content}"
                                        "AUTH", "NOTICE" -> type
                                        else -> frame[1].jsonPrimitive.content
                                    }
                                routes.computeIfAbsent(key) { LinkedBlockingQueue() }.add(frame)
                            }
                            webSocket.request(1)
                            return null
                        }
                    }
                val ws =
                    HttpClient
                        .newHttpClient()
                        .newWebSocketBuilder()
                        .buildAsync(URI(url), listener)
                        .get(10, TimeUnit.SECONDS)
                return Wire(ws, routes)
            }
        }
    }

    // ---- reporting ---------------------------------------------------------------------------------

    private class Report {
        var checks = 0
        val failures = ArrayList<String>()

        fun check(
            name: String,
            ok: Boolean,
            detail: () -> String = { "" },
        ) {
            checks++
            if (!ok) failures += name + detail().takeIf { it.isNotBlank() }?.let { "\n    $it" }.orEmpty()
        }
    }

    /** Runs one section; a section that throws is ONE failure, and the others still run. */
    private fun section(
        report: Report,
        name: String,
        body: () -> Unit,
    ) {
        runCatching(body).onFailure { report.check("section '$name' completed", false) { "${it::class.simpleName}: ${it.message}" } }
    }

    private companion object {
        /** The key whose secret is 1: see the KDoc for the matching RELAY_ADMIN_PUBKEYS npub. */
        val ADMIN_SECRET: ByteArray = Hex.decode("0000000000000000000000000000000000000000000000000000000000000001")

        val NEWEST_FIRST = compareByDescending<Event> { it.createdAt }.thenBy { it.id }

        fun nowSecs() = System.currentTimeMillis() / 1000
    }
}
