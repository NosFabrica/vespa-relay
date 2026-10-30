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

import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation.RelayInformationLimitation
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.tags.NetworkTypeTag
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.tags.RequirementTag
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.tags.RttTag
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.tags.RttType
import com.vitorpamplona.quartz.nip66RelayMonitor.discovery.tags.SupportedNipTag

/**
 * The NIP-66 payload proper: what the relay is, as opposed to the verdict, which is what this
 * deployment decided about it. Each field is either measured on the fitness dial or read off
 * the relay's NIP-11 document, which is a claim the relay makes about itself.
 */
data class RelayFacts(
    /** `clearnet` / `tor`, from the transport that would carry this url, not from the document. */
    val network: String? = null,
    /** Milliseconds from dial to open socket. */
    val rttOpenMs: Long? = null,
    /** Milliseconds from the REQ going out to the relay's first answer. */
    val rttReadMs: Long? = null,
    /**
     * NIP-11's `limitation` keys in NIP-66's spelling (`auth`, `payment`, `pow`, `writes`),
     * negated with a `!` prefix. A relay that told us nothing gets no requirement tags.
     */
    val requirements: List<String> = emptyList(),
    /** NIP-11's `software`, and its `version` behind it. */
    val software: String? = null,
    val version: String? = null,
    /** NIP-11's `supported_nips`. */
    val supportedNips: List<Int> = emptyList(),
) {
    /**
     * The tags, through quartz's NIP-66 codecs. An absent fact writes nothing, and since the writer
     * owns all of these, absence also clears whatever the last pass wrote.
     */
    fun tags(): List<Array<String>> =
        buildList {
            network?.let { add(arrayOf(NETWORK_TAG, it)) }
            rttOpenMs?.let { add(RttTag.assemble(RttType.OPEN, it)) }
            rttReadMs?.let { add(RttTag.assemble(RttType.READ, it)) }
            for (requirement in requirements) add(arrayOf(REQUIREMENT_TAG, requirement))
            software?.let {
                // NIP-66 has no software tag; the version rides third, which a reader of `["s", <software>]` ignores.
                if (version.isNullOrBlank()) add(arrayOf(SOFTWARE_TAG, it)) else add(arrayOf(SOFTWARE_TAG, it, version))
            }
            for (nip in supportedNips) add(SupportedNipTag.assemble(nip))
        }

    companion object {
        const val NETWORK_TAG = NetworkTypeTag.TAG_NAME

        val RTT_OPEN_TAG = RttType.OPEN.tagName

        val RTT_READ_TAG = RttType.READ.tagName

        /** Never written; named so the writer can clear one left by an older pass. */
        val RTT_WRITE_TAG = RttType.WRITE.tagName

        const val REQUIREMENT_TAG = RequirementTag.TAG_NAME

        const val SOFTWARE_TAG = "s"

        const val SUPPORTED_NIP_TAG = SupportedNipTag.TAG_NAME

        /** Every tag the fitness writer replaces on each pass. */
        val OWNED =
            setOf(NETWORK_TAG, RTT_OPEN_TAG, RTT_READ_TAG, RTT_WRITE_TAG, REQUIREMENT_TAG, SOFTWARE_TAG, SUPPORTED_NIP_TAG)

        /** NIP-66's requirement vocabulary; the negative form is how most records say "open to read". */
        const val REQUIREMENT_AUTH = RequirementTag.AUTH

        fun requirement(
            name: String,
            required: Boolean,
        ): String = RequirementTag.assemble(name, negated = !required)[1]

        /** NIP-11's `limitation` block as requirement strings, in quartz's mapping ([RequirementTag.fromNip11]). */
        fun requirementsOf(limitation: RelayInformationLimitation?): List<String> = RequirementTag.fromNip11(limitation).map { RequirementTag.assemble(it)[1] }

        /** The key a requirement is about, with any `!` stripped. */
        fun subjectOf(requirement: String): String = requirement.removePrefix("!")

        /** Everything [advertised], with anything [measured] overriding the claim about the same key. */
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
