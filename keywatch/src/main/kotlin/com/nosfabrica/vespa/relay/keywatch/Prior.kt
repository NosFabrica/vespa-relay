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
package com.nosfabrica.vespa.relay.keywatch

import com.vitorpamplona.quartz.nip01Core.core.HexKey

/** A transition as a report keeps it: every claim and every citation, so a later pass can decide it again. */
class PriorTransition(
    val oldKey: HexKey,
    val newKey: HexKey,
    val status: Status,
    val claims: List<TransitionClaim>,
    val vouches: List<Vouch>,
)

/**
 * What an earlier report holds that the next pass builds on. Read back by [Report.read], which
 * returns none when the report cannot be built on: another format, relay or observer.
 */
class PriorReport(
    val relay: String,
    val observer: HexKey,
    val policy: VouchPolicy,
    /** The pass's own clock when it read: the next one reads from here, less the overlap. */
    val sweptUntil: Long,
    val lastFullPassAt: Long,
    val findings: List<Finding>,
    val transitions: List<PriorTransition>,
)

/** What one pass found that the pass before it had not, or decided differently. */
data class Change(
    val pubkey: HexKey,
    val keyClass: KeyClass,
    val kind: Kind,
    val from: Status?,
    val to: Status,
    val relatedKey: HexKey? = null,
    val activeAfter: Long? = null,
) {
    enum class Kind {
        /** A finding the prior report did not hold: a new leak, claim, secondary or vanish. */
        NEW,

        /** The same finding with another status: a claim validated, a validation contested. */
        STATUS,

        /** A compromised key signed something since the prior report: someone holds it and uses it. */
        SIGNING_AGAIN,
    }

    companion object {
        fun between(
            before: List<Finding>,
            after: List<Finding>,
        ): List<Change> {
            val was = before.associateBy { identity(it) }
            return after.mapNotNull { f ->
                val p = was[identity(f)]
                when {
                    p == null -> {
                        Change(f.pubkey, f.keyClass, Kind.NEW, null, f.status, f.relatedKey, f.activeAfter)
                    }

                    p.status != f.status -> {
                        Change(f.pubkey, f.keyClass, Kind.STATUS, p.status, f.status, f.relatedKey, f.activeAfter)
                    }

                    f.keyClass == KeyClass.COMPROMISED_PUBLIC && f.activeAfter != null && (p.activeAfter ?: Long.MIN_VALUE) < f.activeAfter -> {
                        Change(f.pubkey, f.keyClass, Kind.SIGNING_AGAIN, p.status, f.status, f.relatedKey, f.activeAfter)
                    }

                    else -> {
                        null
                    }
                }
            }
        }

        /** One finding across passes: the same key, class and basis, about the same other key. */
        private fun identity(f: Finding) = listOf(f.pubkey, f.keyClass, f.basis, f.relatedKey)
    }
}
