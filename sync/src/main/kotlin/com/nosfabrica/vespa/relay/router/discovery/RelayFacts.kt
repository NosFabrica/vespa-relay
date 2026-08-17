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
package com.nosfabrica.vespa.relay.router.discovery

/**
 * THE NIP-66 PAYLOAD PROPER — what the relay IS, as opposed to what we decided
 * about it.
 *
 * Every field here is a tag the spec (or the monitors deployed under it) already
 * fixes the meaning of, so a stranger's crawler reads these without knowing this
 * router exists. That is the entire point of filling them in: the verdict is our
 * opinion and lives under a NIP-32 namespace, while THIS is the observation
 * anyone can use.
 *
 * **Leaving them empty was not neutral.** quartz's own convention reads a 30166
 * inside the TTL carrying no `rtt-open` as "checked, could not open" — see
 * [StreamWorld.ownDead], which used to apply that rule and stopped. So every
 * record this monitor signed, `prime` ones included, told every foreign reader
 * applying that convention that the relay was unreachable. A record that says
 * nothing is not silent; it is read as saying something else.
 *
 * ## Where each one comes from
 *
 * Two sources, and the difference is stated per field rather than assumed,
 * because NIP-66 explicitly allows a monitor's measurement to contradict a
 * relay's own document:
 *
 *  - MEASURED on the dial the fitness pass already paid for — [network], the
 *    two rtts, and the auth half of [requirements].
 *  - READ off the relay's NIP-11 document — [software], [supportedNips] and the
 *    rest of [requirements]. There is no way to measure those; a relay's
 *    software name is a claim it makes about itself and nothing more.
 *
 * ## What is deliberately absent
 *
 * `rtt-write` — this router never writes to a relay it monitors, so publishing
 * one would mean inventing it.
 *
 * `g` (geohash) and `T` (relay type) — neither is in NIP-11. Monitors that
 * publish them derive the geohash from IP geolocation (sampled in the wild:
 * a precision ladder `d`/`dp`/`dpz`/`dpz8`, beside `L: host.isp` and
 * `L: host.asn` labels) and the type from an enumeration NIP-66 links to an
 * open issue. Both want a GeoIP database and a classifier this process does not
 * have, and guessing either would put a signed month-long claim about somebody
 * else's server behind a number we made up.
 *
 * `v` (version) — NOT a tag anyone writes. Sampled across 12 monitors and 800
 * records: zero occurrences. NIP-11 does carry a `version` field, so the fact is
 * available, but minting an undefined single letter to hold it is precisely the
 * mistake that put the fitness grade on `s`. It rides [software] instead, in the
 * third element, where a reader that only knows `["s", <software>]` skips it.
 */
data class RelayFacts(
    /**
     * `clearnet` / `tor`, from the transport that would actually carry this url
     * — not from the document, which cannot know.
     *
     * `i2p` and `loki` are in NIP-66's vocabulary and not here: this router has
     * no transport for either, so a url on one is a url we could not have
     * dialled to write a record about.
     */
    val network: String? = null,
    /** Milliseconds from dial to open socket. */
    val rttOpenMs: Long? = null,
    /** Milliseconds from the REQ going out to the relay's first answer. */
    val rttReadMs: Long? = null,
    /**
     * NIP-11's `limitation` keys, in NIP-66's spelling — `auth`, `payment`,
     * `pow`, `writes`, each negated with a `!` prefix.
     *
     * A relay that told us nothing gets NO requirement tags rather than a row
     * of negations: `!auth` is a claim that reads are open, and we would be
     * making it on the strength of a document we never fetched.
     */
    val requirements: List<String> = emptyList(),
    /** NIP-11's `software`, and its `version` behind it. */
    val software: String? = null,
    val version: String? = null,
    /** NIP-11's `supported_nips`. */
    val supportedNips: List<Int> = emptyList(),
) {
    /**
     * The tags, in NIP-66's spelling. Absent facts write NOTHING — never a
     * zero, never an empty string.
     *
     * A `0` rtt would be a relay that answered instantly and an empty `s` a
     * relay running nothing, where the truth in both cases is that this pass
     * did not find out. Since the writer OWNS all of these, an absent fact also
     * clears whatever the last pass wrote, which is what keeps a stale reading
     * from outliving the dial that took it.
     */
    fun tags(): List<Array<String>> =
        buildList {
            network?.let { add(arrayOf(NETWORK_TAG, it)) }
            rttOpenMs?.let { add(arrayOf(RTT_OPEN_TAG, it.toString())) }
            rttReadMs?.let { add(arrayOf(RTT_READ_TAG, it.toString())) }
            for (requirement in requirements) add(arrayOf(REQUIREMENT_TAG, requirement))
            software?.let {
                // The version rides along rather than taking a letter of its
                // own — see the class header. A reader of `["s", <software>]`
                // is unaffected by a third element it does not look at.
                if (version.isNullOrBlank()) add(arrayOf(SOFTWARE_TAG, it)) else add(arrayOf(SOFTWARE_TAG, it, version))
            }
            for (nip in supportedNips) add(arrayOf(SUPPORTED_NIP_TAG, nip.toString()))
        }

    companion object {
        const val NETWORK_TAG = "n"

        const val RTT_OPEN_TAG = "rtt-open"

        const val RTT_READ_TAG = "rtt-read"

        /**
         * Also the tag NIP-66 reserves for `rtt-write`, which is why the name is
         * here without a field: nothing may write one, and a future reader
         * looking for why should find the reason next to the other two.
         */
        const val RTT_WRITE_TAG = "rtt-write"

        const val REQUIREMENT_TAG = "R"

        const val SOFTWARE_TAG = "s"

        const val SUPPORTED_NIP_TAG = "N"

        /**
         * Every tag the fitness writer replaces on each pass.
         *
         * `rtt-write` is in it although nothing writes one: a record carrying a
         * write latency measured by the OLD passive monitor is a reading of a
         * socket nobody has opened since, and the writer that owns the other two
         * rtts is the only thing that will ever be in a position to clear it.
         */
        val OWNED =
            setOf(NETWORK_TAG, RTT_OPEN_TAG, RTT_READ_TAG, RTT_WRITE_TAG, REQUIREMENT_TAG, SOFTWARE_TAG, SUPPORTED_NIP_TAG)

        /**
         * NIP-66's requirement vocabulary, and the `!` that negates it.
         *
         * Sampled in the wild, 800 records: `!auth` on 681, `!pow` on 663,
         * `!payment` on 619, `payment` on 80, `auth` on 18 — so the negative
         * form is not decoration, it is how most records say "open to read".
         */
        const val REQUIREMENT_AUTH = "auth"

        const val REQUIREMENT_PAYMENT = "payment"

        const val REQUIREMENT_POW = "pow"

        const val REQUIREMENT_WRITES = "writes"

        fun requirement(
            name: String,
            required: Boolean,
        ): String = if (required) name else "!$name"

        /** The key a requirement is about, with any `!` stripped. */
        fun subjectOf(requirement: String): String = requirement.removePrefix("!")

        /**
         * The requirements to publish: everything the relay [advertised], with
         * anything we [measured] overriding its claim about the same key.
         *
         * Order follows `measured` first so the record reads with the proven
         * facts in front, and the result carries each key ONCE — a record
         * holding both `auth` and `!auth` says nothing at all, and it is the
         * shape a naive concatenation produces on exactly the relays where the
         * disagreement is the interesting part.
         */
        fun merge(
            measured: List<String>,
            advertised: List<String>,
        ): List<String> {
            val seen = HashSet<String>()
            return buildList {
                for (requirement in measured + advertised) {
                    if (seen.add(subjectOf(requirement))) add(requirement)
                }
            }
        }
    }
}
