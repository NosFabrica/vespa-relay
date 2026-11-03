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
package com.nosfabrica.vespa.relay.maintenance

import com.nosfabrica.vespa.eventstore.VespaEventStore
import com.nosfabrica.vespa.eventstore.trust.TrustReconciler
import kotlinx.coroutines.CancellationException

/**
 * The audit a `VERIFY_TRUST_ON_START` value asks for: null when unset, otherwise check-only unless it
 * is exactly `repair`. Deliberately not a switch: a repair rewrites reputation documents under the
 * writer lock, so an unexpected value must read as the audit that writes nothing.
 */
fun trustVerifyRepair(setting: String?): Boolean? = setting?.trim()?.takeIf { it.isNotEmpty() }?.let { it == "repair" }

/**
 * Hold EVERY reputation document to the kind 10040 and 30382 records stored behind it
 * (`VERIFY_TRUST_ON_START`), and with [repair] re-derive the ones that drifted.
 *
 * The boot reconcile cannot see this: it samples a few cards per named service, which catches a
 * service that was never projected but not one whose cells drifted in part — a card stored with no
 * cell, or a cell left at an older card's score. Ranked reads trust the cell, so such a subject is
 * gated out of every observer's lens that names the service, while the card itself says otherwise.
 * This walks every stored 30382's subject instead: screening is lock-free, and only the suspects are
 * re-judged and (with [repair]) re-derived under the writer lock, so a live relay keeps serving.
 */
suspend fun verifyTrustOnStart(
    store: VespaEventStore,
    repair: Boolean,
    log: (String) -> Unit = ::println,
    nowMs: () -> Long = System::currentTimeMillis,
) {
    val startedMs = nowMs()
    log(
        "trust-verify: ${if (repair) "REPAIRING drift" else "CHECK ONLY — no writes"}" +
            " — every reputation document against the 10040 and 30382 records behind it",
    )
    var lastReport = startedMs
    runCatching {
        store.verifyTrust(repair) { checked ->
            // Throttled: the callback fires per screened batch.
            val now = nowMs()
            if (now - lastReport >= PROGRESS_EVERY_MS) {
                lastReport = now
                log("trust-verify: $checked subject(s) checked in ${(now - startedMs) / 1000}s")
            }
        }
    }.onSuccess { audit ->
        log(trustVerifyReport(audit, (nowMs() - startedMs) / 1000))
    }.onFailure { e ->
        // Cancellation is shutdown, not a failed audit.
        if (e is CancellationException) throw e
        log("trust-verify: FAILED after ${(nowMs() - startedMs) / 1000}s: ${e.message}")
    }
}

/** The audit's one-line verdict: counts, then a few drifted subjects with what was wrong with each. */
internal fun trustVerifyReport(
    audit: TrustReconciler.TrustAudit,
    secs: Long,
): String {
    val head = "trust-verify: ${audit.subjectsChecked} subject(s) and ${audit.parentsChecked} reputation document(s) checked in ${secs}s"
    if (audit.isClean()) return "$head — every document matches its records"
    // Full keys: a pubkey is public, and the operator's next step is /trust/explain/{pubkey}.
    val eg = audit.drift.take(EXAMPLES).joinToString { "${it.subject} (${driftKind(it)})" }
    return "$head — ${audit.driftCount} drifted" +
        (if (audit.repaired) ", all re-derived" else " — set VERIFY_TRUST_ON_START=repair to re-derive them") +
        (if (eg.isNotEmpty()) "; e.g. $eg" else "")
}

/** What [TrustReconciler.TrustDrift] says went wrong, in the words its own KDoc uses. */
internal fun driftKind(drift: TrustReconciler.TrustDrift): String {
    val expected = drift.expected
    val actual = drift.actual?.takeUnless { it.isEmpty() }
    return when {
        actual == null -> {
            "missing: records score it, no document"
        }

        expected == null -> {
            "orphan: a document with no records behind it"
        }

        else -> {
            val stale =
                (expected.influenceScores.keys + actual.influenceScores.keys).count { expected.influenceScores[it] != actual.influenceScores[it] } +
                    (expected.followerCounts.keys + actual.followerCounts.keys).count {
                        expected.followerCounts[it]?.toFloat() != actual.followerCounts[it]?.toFloat()
                    }
            "stale: $stale cell(s) differ"
        }
    }
}

private const val PROGRESS_EVERY_MS = 30_000L
private const val EXAMPLES = 3
