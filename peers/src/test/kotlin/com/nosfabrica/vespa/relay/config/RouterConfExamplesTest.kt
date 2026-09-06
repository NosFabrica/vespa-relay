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
package com.nosfabrica.vespa.relay.config

import com.nosfabrica.vespa.relay.peers.RelayDiscovery
import com.vitorpamplona.quartz.experimental.trustedLists.treasureMap.TrustedListProviderTag
import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ProviderTypes
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.tags.ServiceType
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The shipped example config is the router's documentation, so it has to parse
 * into what it claims to be; a broken example is a broken feature.
 */
class RouterConfExamplesTest {
    /** NIP-85 assertions and Tapestry Trusted Lists: one family, because a search expands all eight. */
    private val trustedKinds = listOf(30382, 30383, 30384, 30385, 30392, 30393, 30394, 30395)

    /** Tests run from the module dir; the example sits at the repo root. */
    private val example: RouterConfig =
        RouterConfigLoader.parse(
            requireNotNull(
                listOf(File("../router.conf.example"), File("router.conf.example")).firstOrNull { it.isFile },
            ) { "missing router.conf.example" }.readText(),
        )

    @Test
    fun `a static stream seeds the store the discovery scans read from`() {
        val static = example.streams.filter { it.discovery == null }
        assertTrue(static.isNotEmpty(), "the example needs at least one statically-addressed stream")
        assertTrue(static.any { it.urls.isNotEmpty() }, "a static stream must name real urls")
        assertEquals(
            example
                .downUpstreams()
                .map { it.streamName }
                .distinct()
                .sorted(),
            static
                .filter { it.urls.isNotEmpty() }
                .map { it.name }
                .distinct()
                .sorted(),
            "downUpstreams() is the static streams only — a discovery one resolves its relays at run time",
        )
    }

    @Test
    fun `the example's budgets fit under the socket ceiling it warns about`() {
        // Duplicated rather than imported from `VisitPool`: `:peers` does not see `:sync`.
        val dispatcherHeadroom = 900
        val dials = example.streams.sumOf { it.visitConcurrency ?: RouterConfig.DEFAULT_VISIT_CONCURRENCY }
        val tails = example.streams.sumOf { it.maxLiveConcurrency ?: RouterConfig.DEFAULT_MAX_LIVE_CONCURRENCY }
        assertTrue(
            dials + tails <= dispatcherHeadroom,
            "the example asks for up to $dials dial(s) and $tails tail(s) = ${dials + tails} sockets, over the " +
                "$dispatcherHeadroom this router leaves for the static upstreams, the monitor and the healer. " +
                "Lower a stream's visitConcurrency or maxLiveConcurrency",
        )
    }

    @Test
    fun `every discovery scan reads a kind some stream actually mirrors`() {
        // A scan for a kind nothing mirrors fails silently: no error, just no relays.
        val mirrored = example.streams.flatMap { it.filter.kinds.orEmpty() }.toSet()
        example.discoveryStreams().forEach { stream ->
            // Kind 30166 excepted: the monitor writes those into this store itself.
            stream.discovery!!.sources.filterNot { it.filter.kinds == listOf(30166) }.forEach { source ->
                source.filter.kinds.orEmpty().forEach { kind ->
                    assertTrue(kind in mirrored, "stream '${stream.name}' scans kind $kind, which no stream mirrors")
                }
            }
        }
    }

    @Test
    fun `the monitor fans out over NIP-65 write relays`() {
        val sources = example.monitor!!.sources.filter { it.filter.kinds == listOf(10002) }
        assertTrue(sources.isNotEmpty(), "the monitor does not read NIP-65 lists")

        for (source in sources) {
            val nip65 = source.selects.single()
            assertEquals(10002, nip65.kind)
            assertEquals("r", nip65.tag)
            // `marker = "write"` is marked write, marked empty, or too short to carry a marker.
            assertEquals(1, nip65.urlIndex)
            assertEquals(
                listOf(
                    TagCondition(index = 2, equals = "write"),
                    TagCondition(index = 2, equals = ""),
                    TagCondition(maxSize = 2),
                ),
                nip65.where,
            )
        }
    }

    @Test
    fun `the monitor also fans out over NIP-29 group hosts`() {
        val hosts =
            example.monitor!!
                .sources
                .filter { it.filter.kinds == listOf(10009) }
                .map { "monitor" to it }
        assertTrue(hosts.isNotEmpty(), "the monitor does not read NIP-29 group lists")

        for ((stream, source) in hosts) {
            val select = source.selects.single()
            assertEquals(10009, select.kind, "the scan is narrowed to the group list kind")
            assertEquals("group", select.tag)
            assertEquals(2, select.urlIndex, "a `group` tag carries the host relay at element 2, after the id")
            assertTrue(select.where.isEmpty(), "a group tag has no marker to test — every entry names a host")
            assertTrue(
                select.bindings.isEmpty(),
                "the select is deliberately unbound: binding the id would ask each host only for the listed " +
                    "groups, and give up the tag projection for a paging scan over whole events",
            )
            assertTrue(
                10009 in example.streams.flatMap { it.filter.kinds.orEmpty() },
                "$stream scans kind 10009, so some stream has to mirror it",
            )
        }

        // Group posts are kinds 9, 11 and 1111; 39000 is the group's own record.
        assertTrue(
            example.streams.any {
                it.discovery?.sources?.any { s -> s.filter.kinds == listOf(30166) } == true &&
                    it.filter.kinds
                        .orEmpty()
                        .containsAll(listOf(9, 11, 1111, 39000))
            },
            "the monitor certifies group hosts but no verdict-built stream asks for group posts or the group record",
        )
    }

    @Test
    fun `a stream that mirrors content mirrors the retractions too`() {
        // Kind 5 and kind 62 retract what kind 1 wrote, and the store enforces both at insert.
        val content = example.streams.filter { it.filter.kinds?.contains(1) == true }
        assertTrue(content.isNotEmpty(), "the example mirrors no user-written content at all")
        content.forEach { stream ->
            val kinds = stream.filter.kinds.orEmpty()
            assertTrue(5 in kinds, "stream '${stream.name}' mirrors notes but not the kind 5 that deletes them")
            assertTrue(62 in kinds, "stream '${stream.name}' mirrors notes but not the kind 62 that vanishes their author")
        }
    }

    @Test
    fun `a stream that audits re-walks far less often than it audits`() {
        // A visit pages before it audits, so equal periods re-page and then reconcile the same ground.
        example.streams.filter { it.negentropySyncThePastSeconds != null && it.refetchThePastSeconds != null }.forEach {
            assertTrue(
                it.refetchThePastSeconds!! > it.negentropySyncThePastSeconds!!,
                "stream '${it.name}' re-fetches the past every ${it.refetchThePastSeconds}s against a reconcile " +
                    "every ${it.negentropySyncThePastSeconds}s",
            )
        }
        // The two outbox streams are the ones a re-walk costs most: every certified relay, most kinds.
        listOf("profileViaOutbox", "contentViaOutbox").forEach { name ->
            assertEquals(
                2_592_000L,
                example.streams.single { it.name == name }.refetchThePastSeconds,
                "stream '$name' should re-walk monthly, not on the router's weekly default",
            )
        }
    }

    @Test
    fun `the assertions stream names the NIP-85 services it wants`() {
        val assertions = example.discoveryStreams().first { it.name == "assertions" }
        val source = assertions.discovery!!.sources.single()
        assertTrue(assertions.urls.isEmpty(), "a relaySource stream carries no static urls")
        assertEquals(listOf(10040), source.filter.kinds)
        // Both delegation shapes, NIP-85's `kind:metric` and Tapestry's bare kind, put the pubkey at 1 and the url at 2.
        assertTrue(source.selects.all { it.urlIndex == 2 })
        assertTrue(
            source.selects.all { it.tag != null },
            "every select NAMES its delegation tag — see `the 10040 scans read delegations and nothing else`",
        )
        // Declarations only: a kind a provider relay never serves re-opens a leg over the whole past.
        assertEquals(trustedKinds, assertions.filter.kinds)
        // A kind mirrored but not owned is one nothing reconciles.
        assertEquals(trustedKinds.toSet(), assertions.ownedKinds)
        // The service sits at slot 1 of the tag whose slot 2 named the url; anywhere else is the cross product.
        assertTrue(
            source.selects.all { it.bindings["authors"] == BindingSlot.OfTag(1) },
            "each service tag binds its own provider as the authors to ask for",
        )
        // A 10040 is as writable as a 10002: its dead urls cost the monitor a probe, not this stream a dial.
        assertTrue(
            example.streams
                .single { it.name == "assertions" }
                .discovery!!
                .gatedBy
                .isNotEmpty(),
            "the assertions stream dials only relays a verdict vouches for",
        )
        // Which only works if those urls earn verdicts: the monitor must read the same 10040 tags.
        val monitor10040 = example.monitor!!.sources.filter { it.filter.kinds == listOf(10040) }
        assertTrue(monitor10040.isNotEmpty(), "the assertions scan is gated on verdicts no monitor source would ever take")
        assertEquals(
            example
                .discoveryStreams()
                .first { it.name == "assertions" }
                .discovery!!
                .sources
                .single()
                .selects
                .mapNotNull { it.tag }
                .toSet(),
            monitor10040.flatMap { it.selects }.mapNotNull { it.tag }.toSet(),
            "the monitor must read the SAME 10040 tags the assertions stream scans, or it certifies relays that " +
                "stream never asks and leaves ones it does ask uncertified",
        )
        assertTrue(
            monitor10040.flatMap { it.selects }.all { it.urlIndex == 2 },
            "the service tag carries the url at element 2 — reading 1 would probe provider pubkeys as urls",
        )
    }

    /**
     * The monitor's 10040 scan and the assertions stream's, checked apart:
     * a rule asserted over the union passes while one of the two is missing a tag.
     */
    private fun scans10040() =
        mapOf(
            "monitor" to
                example.monitor!!
                    .sources
                    .filter { it.filter.kinds == listOf(10040) }
                    .flatMap { it.selects },
            "assertions" to
                example
                    .discoveryStreams()
                    .first { it.name == "assertions" }
                    .discovery!!
                    .sources
                    .single()
                    .selects,
        )

    private fun map10040(vararg tags: Array<String>) = NostrSignerSync().sign<Event>(1_700_000_000L, 10040, arrayOf(*tags), "")

    @Test
    fun `the 10040 scans name every delegation quartz defines`() {
        // A metric added to ProviderTypes and not here is a provider this router never discovers.
        val declared =
            ProviderTypes.javaClass.methods
                .filter { it.parameterCount == 0 && it.returnType == ServiceType::class.java }
                .mapNotNull { runCatching { it.invoke(ProviderTypes) as ServiceType }.getOrNull() }
                .map { it.toValue() }
                .toSet() + TrustedListProviderTag.KINDS.map { it.toString() }

        scans10040().forEach { (name, selects) ->
            assertEquals(
                declared.sorted(),
                selects.mapNotNull { it.tag }.distinct().sorted(),
                "the '$name' 10040 scan and quartz disagree on the delegation vocabulary",
            )
        }
    }

    @Test
    fun `the 10040 scans read delegations and nothing else`() {
        // A hint has a delegation's shape, hex at 1 and a url at 2; read as one it binds an event id as an author.
        val provider = "a".repeat(64)

        scans10040().forEach { (name, selects) ->
            for (service in listOf("30382:rank", "30383:zap_cnt", "30385:reaction_cnt", "30392", "30395")) {
                val map = map10040(arrayOf(service, provider, "wss://provider.example"))
                assertEquals(
                    listOf("wss://provider.example/"),
                    selects.flatMap { RelayDiscovery.urlsIn(map, it) }.map { it.url }.distinct(),
                    "the '$name' scan cannot see a `$service` delegation, so that provider is never dialled",
                )
            }
        }

        val noise =
            map10040(
                arrayOf("p", provider, "wss://hint.example"),
                arrayOf("e", "b".repeat(64), "wss://hint.example"),
                arrayOf("alt", "a trust provider list"),
                arrayOf("d", "map"),
                arrayOf("client", "someclient", "wss://hint.example"),
            )
        scans10040().forEach { (name, selects) ->
            assertEquals(
                emptyList(),
                selects.flatMap { RelayDiscovery.urlsIn(noise, it) }.map { it.url }.distinct(),
                "the '$name' scan reads a relay HINT as a delegation — which binds a stranger's pubkey, or an " +
                    "event id, as a provider to ask for",
            )
        }
    }

    @Test
    fun `a stream that mirrors git objects mirrors the statuses that close them`() {
        // The store indexes all four NIP-34 status kinds; a patch whose status never arrives reads as open forever.
        val statuses = listOf(1630, 1631, 1632, 1633)
        example.streams.forEach { stream ->
            val kinds = stream.filter.kinds.orEmpty()
            // 1617 patches, 1621 issues: the objects a status is about.
            if (1617 in kinds || 1621 in kinds) {
                assertEquals(
                    statuses,
                    statuses.filter { it in kinds },
                    "stream '${stream.name}' mirrors git patches or issues but not every status that resolves them",
                )
            }
        }
    }

    @Test
    fun `a stream that mirrors one trusted declaration mirrors the whole family`() {
        // A search expands the family, so a partial mirror is a silent half-feature.
        example.streams.forEach { stream ->
            val kinds = stream.filter.kinds.orEmpty()
            val carried = trustedKinds.filter { it in kinds }
            if (carried.isNotEmpty()) {
                assertEquals(
                    trustedKinds,
                    carried,
                    "stream '${stream.name}' mirrors ${carried.size} of the 8 trusted declaration kinds — a search " +
                        "expanding the missing ${trustedKinds.filterNot { it in kinds }} would resolve into an empty store",
                )
            }
        }
        // Somebody has to carry them, or the rule above is vacuous.
        assertTrue(
            example.streams.any { s -> trustedKinds.all { it in s.filter.kinds.orEmpty() } },
            "no stream mirrors the trusted declaration kinds at all",
        )
    }

    @Test
    fun `every dynamic stream states how often its list is re-derived`() {
        example.discoveryStreams().forEach { stream ->
            assertTrue(stream.discovery!!.refreshSeconds > 0, "'${stream.name}' needs a refresh period")
        }
    }
}
