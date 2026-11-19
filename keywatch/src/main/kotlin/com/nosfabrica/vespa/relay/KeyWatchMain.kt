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

import com.nosfabrica.vespa.relay.keywatch.KeyWatch
import com.nosfabrica.vespa.relay.keywatch.KeyWatchConfig
import com.nosfabrica.vespa.relay.keywatch.PriorReport
import com.nosfabrica.vespa.relay.keywatch.RelayCorpus
import com.nosfabrica.vespa.relay.keywatch.Report
import com.nosfabrica.vespa.relay.keywatch.VouchPolicy
import com.vitorpamplona.quartz.nip01Core.relay.client.NostrClient
import com.vitorpamplona.quartz.nip01Core.relay.filters.Filter
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.RelayUrlNormalizer
import com.vitorpamplona.quartz.nip01Core.relay.sockets.okhttp.BasicOkHttpWebSocket
import com.vitorpamplona.quartz.nip85TrustedAssertions.list.TrustProviderListEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import okhttp3.OkHttpClient
import java.io.File
import java.time.Duration
import kotlin.system.exitProcess

/** Every line this process prints starts with it. */
private const val LOG = "keywatch"

/** search.brainstorm.world: production, read-only. Never publish to it. */
private const val DEFAULT_RELAY = "wss://search.brainstorm.world/"

/** The canonical public observer, whose 10040 names a provider whose cards are on that relay. */
private const val DEFAULT_OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"

private val prettyJson = Json { prettyPrint = true }

/**
 * Reads one relay and writes a JSON report of keys whose secrets are public, keys that say they
 * moved, and whether third parties admitted the move. It writes nothing to any relay.
 *
 *   KEYWATCH_RELAY                 the relay to read (default search.brainstorm.world)
 *   KEYWATCH_READ_TOKEN            appended to every filter's search (default `include:spam`)
 *   KEYWATCH_OBSERVER              hex pubkey whose rank provider ranks vouchers
 *   KEYWATCH_OUT                   report path (default keywatch-report.json)
 *   KEYWATCH_INTERVAL_MINUTES      0 runs once (default); otherwise repeats
 *   KEYWATCH_MIN_VOUCHER_RANK      default 50
 *   KEYWATCH_MIN_VOUCHERS          default 2
 *   KEYWATCH_VOUCHER_ABOVE_NEW_KEY default true
 *   KEYWATCH_REQUIRE_KNEW_OLD_KEY  default false
 *   KEYWATCH_MAX_DEGRADED_WAIT_MINUTES  how long to wait for a partial index to heal (default 60)
 *   KEYWATCH_FULL                  true reads the whole corpus even when KEYWATCH_OUT holds a report to build on
 *   KEYWATCH_FULL_EVERY_DAYS       a full pass at least this often (default 30): only a full pass finds backdated events
 *   KEYWATCH_OVERLAP_HOURS         how far before the prior report's cursor an incremental pass reads (default 48)
 *
 * The report at KEYWATCH_OUT is also the state: a pass that finds one it can build on reads only
 * what arrived since and decides again only what can still change (docs/keywatch.md, Updating).
 */
fun main() {
    val env = System.getenv()
    val relay = RelayUrlNormalizer.normalizeOrNull(env["KEYWATCH_RELAY"] ?: DEFAULT_RELAY) ?: fail("KEYWATCH_RELAY is not a relay url")
    val observer = (env["KEYWATCH_OBSERVER"] ?: DEFAULT_OBSERVER).lowercase()
    if (!observer.matches(Regex("[0-9a-f]{64}"))) fail("KEYWATCH_OBSERVER must be a 64-hex public key")
    val out = File(env["KEYWATCH_OUT"] ?: "keywatch-report.json")
    val intervalMinutes = env.long("KEYWATCH_INTERVAL_MINUTES", 0)
    val policy =
        VouchPolicy(
            minVoucherRank = env.long("KEYWATCH_MIN_VOUCHER_RANK", 50).toInt(),
            minVouchers = env.long("KEYWATCH_MIN_VOUCHERS", 2).toInt(),
            aboveNewKey = env.bool("KEYWATCH_VOUCHER_ABOVE_NEW_KEY", true),
            requireKnewOldKey = env.bool("KEYWATCH_REQUIRE_KNEW_OLD_KEY", false),
        )
    val config =
        KeyWatchConfig(
            observer = observer,
            policy = policy,
            maxDegradedWaitMs = env.long("KEYWATCH_MAX_DEGRADED_WAIT_MINUTES", 60) * 60_000,
            overlapSeconds = env.long("KEYWATCH_OVERLAP_HOURS", 48) * 3_600,
        )
    val forceFull = env.bool("KEYWATCH_FULL", false)
    val fullEverySeconds = env.long("KEYWATCH_FULL_EVERY_DAYS", 30) * 86_400

    val okhttp = OkHttpClient.Builder().connectTimeout(Duration.ofSeconds(20)).build()
    val scope = CoroutineScope(SupervisorJob())
    val client = NostrClient(BasicOkHttpWebSocket.Builder { okhttp }, scope)
    // The canary only has to be a COUNT: the relay refuses every one while its index is partial.
    val canary = Filter(kinds = listOf(TrustProviderListEvent.KIND), authors = listOf(observer))
    val corpus = RelayCorpus(client, relay, env["KEYWATCH_READ_TOKEN"] ?: "include:spam", canary)
    val log: (String) -> Unit = { System.err.println("$LOG: $it") }

    log("reading ${relay.url} (read only); report to ${out.absolutePath}")
    try {
        runBlocking {
            while (true) {
                val started = System.currentTimeMillis()
                val now = started / 1000
                val prior = if (forceFull) null.also { log("KEYWATCH_FULL=true: running a full pass") } else priorReport(out, relay.url, observer, now, fullEverySeconds, log)
                val result = KeyWatch(corpus, config, log).run(now, prior)
                writeAtomically(out, prettyJson.encodeToString(Report.toJson(result, relay.url, observer, policy)))
                log("${result.findings.size} findings over ${result.findings.map { it.pubkey }.toSet().size} keys in ${(System.currentTimeMillis() - started) / 1000}s")
                if (intervalMinutes <= 0) break
                delay(intervalMinutes * 60_000)
            }
        }
    } finally {
        corpus.close()
        runCatching { client.disconnect() }
        scope.cancel()
        okhttp.dispatcher.executorService.shutdown()
        okhttp.connectionPool.evictAll()
    }
}

/** The report a previous pass left at [out], if this pass can build on it. */
private fun priorReport(
    out: File,
    relay: String,
    observer: String,
    nowSeconds: Long,
    fullEverySeconds: Long,
    log: (String) -> Unit,
): PriorReport? {
    if (!out.isFile) {
        log("no report at ${out.path} yet: running a full pass")
        return null
    }
    val json = runCatching { Json.parseToJsonElement(out.readText()) as? JsonObject }.getOrNull()
    if (json == null) {
        log("${out.path} is not a report: running a full pass")
        return null
    }
    return Report.read(json, relay, observer, nowSeconds, fullEverySeconds, log)
}

/** A reader never sees half a report: write beside it, then rename over it. */
private fun writeAtomically(
    target: File,
    text: String,
) {
    target.absoluteFile.parentFile?.let { if (!it.isDirectory && !it.mkdirs()) error("cannot create ${it.path}") }
    val tmp = File(target.absoluteFile.parentFile, ".${target.name}.tmp")
    tmp.writeText(text)
    if (!tmp.renameTo(target)) error("cannot move ${tmp.path} onto ${target.path}")
}

private fun Map<String, String>.long(
    name: String,
    default: Long,
): Long = this[name]?.let { it.toLongOrNull() ?: fail("$name must be a number, not `$it`") } ?: default

private fun Map<String, String>.bool(
    name: String,
    default: Boolean,
): Boolean = this[name]?.let { it.toBooleanStrictOrNull() ?: fail("$name must be true or false, not `$it`") } ?: default

private fun fail(message: String): Nothing {
    System.err.println("$LOG: $message")
    exitProcess(2)
}
