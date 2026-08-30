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
 * into what it claims to be — a broken example is a broken feature.
 */
class RouterConfExamplesTest {
    /**
     * The eight kinds a trust service DECLARES, and the unit they travel in.
     *
     * NIP-85 assertions (30382-30385) and Tapestry Trusted Lists (30392-30395)
     * are the same statement about the same four subjects — a user, an event,
     * an addressable event, an external id — in two vocabularies. A search
     * EXPANDS all eight: a hit on one is answered with the records it points
     * at. So they are a family here, not eight independent dials.
     */
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
        // A relaySource stream reads its relays out of events we already hold, so
        // something with hand-written urls has to put the first ones there. Only
        // static streams can: a store with no relay lists gives every discovery
        // stream an empty fan-out, forever.
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
        // THE EXAMPLE IS WHAT AN OPERATOR COPIES, so it must not be a config
        // that trips the router's own boot warning. It did: the dial width and
        // the tail budget were router-wide numbers that summed to 728 against
        // a headroom of 900, and moving them inside the streams turned two
        // uncapped streams into two FULL default budgets — the same file then
        // asked for 2,068 sockets, and every new connect on that deployment
        // would queue behind ones already held.
        //
        // The numbers are duplicated here rather than imported from
        // `VisitPool`: `:peers` does not see `:sync`, and a ceiling asserted
        // against itself asserts nothing anyway.
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
        // The chain in the example is static(10002) -> outbox(10040) -> assertions.
        // A scan for a kind nothing mirrors is a stream that can never fan out,
        // and it fails silently — there is no error, just no relays.
        val mirrored = example.streams.flatMap { it.filter.kinds.orEmpty() }.toSet()
        example.discoveryStreams().forEach { stream ->
            // Kind 30166 excepted: those are the monitor's own records, which
            // it WRITES into this store — no stream mirrors them, and none
            // needs to.
            stream.discovery!!.sources.filterNot { it.filter.kinds == listOf(30166) }.forEach { source ->
                source.filter.kinds.orEmpty().forEach { kind ->
                    assertTrue(kind in mirrored, "stream '${stream.name}' scans kind $kind, which no stream mirrors")
                }
            }
        }
    }

    @Test
    fun `the monitor fans out over NIP-65 write relays`() {
        // Found by SHAPE, not by name — and in the MONITOR block now: relay
        // list parsing moved off the streams and onto the monitor's own
        // sources, whose verdicts the streams then select on. The shape checks
        // survive the move because they were never about which block the
        // source lives in.
        val sources = example.monitor!!.sources.filter { it.filter.kinds == listOf(10002) }
        assertTrue(sources.isNotEmpty(), "the monitor does not read NIP-65 lists")

        for (source in sources) {
            val nip65 = source.selects.single()
            assertEquals(10002, nip65.kind)
            assertEquals("r", nip65.tag)
            // 10002 puts the url first and its marker after it; only the write side
            // is where a user's own events land, which is what an outbox mirror wants.
            // The example says `marker = "write"`, which is sugar for exactly this:
            // marked write, marked empty, or too short to carry a marker at all.
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
        // By shape again: a scan of kind 10009. The `group` tag is the one
        // relay list in the protocol that does not put the url at element 1 —
        // it is ["group", <id>, <relay url>, <name?>] — so the whole point of
        // this test is that the example says 2 and not 1. Reading element 1
        // would hand the fan-out a set of GROUP IDS to dial, which normalize
        // rejects one at a time and silently: no error, no relays, and a
        // `group:` search with nothing behind it.
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

        // The half that makes it worth doing: a host discovered this way is
        // only useful if something asks it for what a group actually holds.
        // NIP-29 posts are kinds 9 (chat) and 11 (thread) with replies in 1111,
        // and the group's own record is 39000 — the one the `group:` picker
        // resolves a name against.
        // The hosts the monitor certifies are only useful if a visit-mode
        // stream then asks them for what a group actually holds.
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
        // By SHAPE again: a stream carrying kind 1 is mirroring what people
        // write, and one that takes the notes without the kind 5 (NIP-09) and
        // kind 62 (NIP-62) that retract them goes on serving what its authors
        // deleted. The store enforces both at insert, so mirroring them is the
        // whole mechanism — and the STORED request is what the next cycle's
        // re-download is checked against, without which the erase is undone on
        // the following walk.
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
        // The two are the same check at different prices — the audit
        // reconciles the covered history, the re-walk re-downloads it — and a
        // visit pages BEFORE it audits, so equal periods mean the stream pages
        // its whole history and then reconciles the same ground in one visit.
        example.streams.filter { it.negentropySyncThePastSeconds != null && it.refetchThePastSeconds != null }.forEach {
            assertTrue(
                it.refetchThePastSeconds!! > it.negentropySyncThePastSeconds!!,
                "stream '${it.name}' re-fetches the past every ${it.refetchThePastSeconds}s against a reconcile " +
                    "every ${it.negentropySyncThePastSeconds}s",
            )
        }
        // The two outbox streams are the ones it costs most: every certified
        // relay, ~140 kinds on the content mirror.
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
        // Every select reads the url AFTER the provider pubkey, and NAMES NO
        // TAG. A 10040 delegates in two shapes — NIP-85's `kind:metric`
        // (`["30382:rank", <pubkey>, <relay>]`, 34 of them across 30382-30385
        // in quartz's ProviderTypes) and Tapestry's bare kind (`["30392",
        // <pubkey>, <relay>]`) — and both put the pubkey at 1 and the url at
        // 2. Naming them would be 38 entries that the next metric outdates
        // silently, which is the failure this stream cannot report: a provider
        // never discovered is not an error, just a relay never dialled.
        assertTrue(source.selects.all { it.urlIndex == 2 })
        assertTrue(
            source.selects.all { it.tag != null },
            "every select NAMES its delegation tag — see `the 10040 scans read delegations and nothing else`",
        )
        // The stream asks for the declarations and NOTHING else: a provider
        // relay serves no kind 0 or 10002 (measured, 12 pairs), and a kind that
        // never returns an event never earns a band span — so asking for one
        // re-opens a leg over the whole past at every visit, per (relay,
        // provider), forever.
        assertEquals(trustedKinds, assertions.filter.kinds)
        // Every kind it asks for is a kind its upstream owns, which is what
        // lets the retraction audit's band land on the same key the catch-up
        // reads — see `RetractionAudit`. Owning the whole family is what makes
        // `deleteMissing` mean anything on it: a kind mirrored but not owned is
        // one nothing ever reconciles.
        assertEquals(trustedKinds.toSet(), assertions.ownedKinds)
        // ...and only from the services the SAME tag paired with each relay. The
        // service sits at slot 1 of the tag whose slot 2 named the url, so the
        // two travel together; binding it from anywhere else would be the cross
        // product wearing the right shape.
        assertTrue(
            source.selects.all { it.bindings["authors"] == BindingSlot.OfTag(1) },
            "each service tag binds its own provider as the authors to ask for",
        )
        // A band per (relay, service) rather than per relay — the pool makes
        // one ask per bound author, structurally.
        // GATED on the monitor's verdicts: a 10040 is as writable as a 10002,
        // and at millions of provider lists the spammed dead urls in them must
        // cost the monitor one probe each — never this stream a dial and a
        // timeout per cycle forever.
        assertTrue(
            example.streams
                .single { it.name == "assertions" }
                .discovery!!
                .gatedBy
                .isNotEmpty(),
            "the assertions stream dials only relays a verdict vouches for",
        )
        // ...which only works if those urls EARN verdicts: the monitor must
        // read the same 10040 tags as candidates.
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
     * The two 10040 scans, kept APART: the monitor's candidate source and the
     * assertions stream's own. Checked separately, never unioned — a rule
     * asserted over the union passes while one of the two is missing a tag,
     * which is the hole this shape exists to close.
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
        // THE LIST IS LONG AND HAND-WRITTEN, so it is held to its source
        // rather than trusted. NIP-85's vocabulary is `kind:metric` and grows
        // upstream — a metric added to ProviderTypes and not added here is a
        // provider this router never discovers, and nothing about that failure
        // is visible: no error, no warning, just a relay never dialled. This
        // is what makes naming the tags safe instead of a slow leak.
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
        // THE BEHAVIOUR THE NAMES BUY, run through the example's OWN selects.
        // A delegation and a relay hint are the same SHAPE — three elements,
        // hex at 1, a url at 2 — so a scan that took every tag with a url at 2
        // would read `p` and `e` hints as delegations. On the assertions
        // stream that is worse than noise: its `authors` binding would bind an
        // EVENT ID as the author to ask for, which is a band per bogus
        // (relay, author) pair that can never return an event.
        val provider = "a".repeat(64)

        // Both delegation shapes are read, by whichever select names them —
        // and by EACH scan, since the two are dialled for different reasons.
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

        // ...and nothing else in a 10040 is read as one.
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
        // Same shape as the retraction rule above, and found the same way: the
        // store INDEXES all four NIP-34 status kinds, and the example mirrored
        // only 1630. A patch or issue whose status never arrives reads as open
        // forever — and 1631/1632/1633 were searchable kinds that no stream
        // fetched at all, so nothing could ever match them.
        val statuses = listOf(1630, 1631, 1632, 1633)
        example.streams.forEach { stream ->
            val kinds = stream.filter.kinds.orEmpty()
            // 1617 patches, 1621 issues — the objects a status is ABOUT.
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
        // A SEARCH EXPANDS THESE: a hit on a Trusted List or an assertion is
        // answered with the records it points at, and a pointer resolves
        // against what this store holds. Mirroring part of the family is the
        // silent half-feature — the expansion runs, finds nothing, and reads
        // to a client exactly like a relay with no lists on it.
        //
        // By SHAPE, not by stream name: whichever stream carries one of the
        // eight has to carry all eight, so a future stream inherits the rule.
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
        // And somebody has to carry them, or the rule above is vacuous.
        assertTrue(
            example.streams.any { s -> trustedKinds.all { it in s.filter.kinds.orEmpty() } },
            "no stream mirrors the trusted declaration kinds at all",
        )
    }

    @Test
    fun `every dynamic stream states how often its list is re-derived`() {
        // A dead relay is caught by the client's idle timeout in seconds; the
        // one clock the config owes discovery is how often a scan's list is
        // re-read from the store.
        example.discoveryStreams().forEach { stream ->
            assertTrue(stream.discovery!!.refreshSeconds > 0, "'${stream.name}' needs a refresh period")
        }
    }
}
