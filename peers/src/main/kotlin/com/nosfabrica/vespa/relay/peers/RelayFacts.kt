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
package com.nosfabrica.vespa.relay.peers

/**
 * The NIP-66 payload proper: what the relay is, as opposed to the verdict,
 * which is what this deployment decided about it. Every field is a tag whose
 * meaning the spec already fixes, so a stranger's crawler reads them without
 * knowing this router exists.
 *
 * Two sources, stated per field: measured on the fitness pass's dial
 * ([network], the rtts, the auth half of [requirements]) or read off the
 * relay's NIP-11 document ([software], [supportedNips], the rest). A relay's
 * software name is a claim it makes about itself and nothing more.
 *
 * Deliberately absent: `rtt-write` (this router never writes to a relay it
 * monitors), `g` and `T` (need a GeoIP database and a classifier this process
 * does not have), and a `v` tag (not a tag anyone writes; the version rides
 * as the third element of [software] instead).
 */
data class RelayFacts(
    /** `clearnet` / `tor`, from the transport that would carry this url, not from the document. */
    val network: String? = null,
    /** Milliseconds from dial to open socket. */
    val rttOpenMs: Long? = null,
    /** Milliseconds from the REQ going out to the relay's first answer. */
    val rttReadMs: Long? = null,
    /**
     * NIP-11's `limitation` keys in NIP-66's spelling (`auth`, `payment`,
     * `pow`, `writes`), each negated with a `!` prefix. A relay that told us
     * nothing gets no requirement tags: `!auth` claims reads are open.
     */
    val requirements: List<String> = emptyList(),
    /** NIP-11's `software`, and its `version` behind it. */
    val software: String? = null,
    val version: String? = null,
    /** NIP-11's `supported_nips`. */
    val supportedNips: List<Int> = emptyList(),
) {
    /**
     * The tags, in NIP-66's spelling. An absent fact writes nothing, never a
     * zero or an empty string; since the writer owns all of these, absence also
     * clears whatever the last pass wrote.
     */
    fun tags(): List<Array<String>> =
        buildList {
            network?.let { add(arrayOf(NETWORK_TAG, it)) }
            rttOpenMs?.let { add(arrayOf(RTT_OPEN_TAG, it.toString())) }
            rttReadMs?.let { add(arrayOf(RTT_READ_TAG, it.toString())) }
            for (requirement in requirements) add(arrayOf(REQUIREMENT_TAG, requirement))
            software?.let {
                // A reader of `["s", <software>]` is unaffected by a third element.
                if (version.isNullOrBlank()) add(arrayOf(SOFTWARE_TAG, it)) else add(arrayOf(SOFTWARE_TAG, it, version))
            }
            for (nip in supportedNips) add(arrayOf(SUPPORTED_NIP_TAG, nip.toString()))
        }

    companion object {
        const val NETWORK_TAG = "n"

        const val RTT_OPEN_TAG = "rtt-open"

        const val RTT_READ_TAG = "rtt-read"

        /** Named without a field: nothing may write one, but the writer must be able to clear one. */
        const val RTT_WRITE_TAG = "rtt-write"

        const val REQUIREMENT_TAG = "R"

        const val SOFTWARE_TAG = "s"

        const val SUPPORTED_NIP_TAG = "N"

        /** Every tag the fitness writer replaces on each pass; `rtt-write` is in it so a stale one is cleared. */
        val OWNED =
            setOf(NETWORK_TAG, RTT_OPEN_TAG, RTT_READ_TAG, RTT_WRITE_TAG, REQUIREMENT_TAG, SOFTWARE_TAG, SUPPORTED_NIP_TAG)

        /** NIP-66's requirement vocabulary; the negative form is how most records say "open to read". */
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
         * The requirements to publish: everything [advertised], with anything
         * [measured] overriding the claim about the same key. Each key appears
         * once, measured first; a record holding both `auth` and `!auth` says nothing.
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
