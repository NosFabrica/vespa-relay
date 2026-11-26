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
package com.nosfabrica.vespa.relay.sync

import com.vitorpamplona.quartz.nip01Core.core.Event
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocket
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebSocketListener
import com.vitorpamplona.quartz.nip01Core.relay.sockets.WebsocketBuilder
import com.vitorpamplona.quartz.nip01Core.store.IdAndTime
import com.vitorpamplona.quartz.nip50Search.SearchQuery
import com.vitorpamplona.quartz.nip77Negentropy.NegentropyServerSession
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.longOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/**
 * A scripted vespa-relay with `REQUIRE_READ_LENS`, over quartz's socket seam: it challenges every
 * connection, refuses an anonymous read that waives nothing naming `include:spam`, and answers a
 * signed-in one through that key's lens, which trusts nobody, so empty. With [membersOnly] it first
 * refuses every unsigned read for a reason that names no waiver; with [membersWrite], every unsigned EVENT. NEG-OPEN runs quartz's own server
 * session over the set the read would serve.
 */
internal class GatedPeer(
    private val scope: CoroutineScope,
    private val corpus: List<Event>,
    private val membersOnly: Boolean = false,
    private val gated: Boolean = true,
    /** Refuses an EVENT from an unsigned connection `auth-required:`, as a members-only write relay does. */
    private val membersWrite: Boolean = false,
    /** Answers an unsigned REQ with nothing and a NIP-67 `["EOSE",sub,["auth"]]`, the whole set once signed in. */
    private val authHint: Boolean = false,
    /** False: a REQ by ids it would serve is never answered, as a relay that names ids it cannot load. */
    private val answersIds: Boolean = true,
    /** How long it holds a by-id answer it serves, so overlapping fetches can be seen. */
    private val idsDelayMs: Long = 0,
) : WebsocketBuilder {
    /** AUTH frames received, over every connection. */
    val auths = AtomicInteger()

    /** Events published to it, accepted. */
    val published = CopyOnWriteArrayList<String>()

    /** Every filter a read carried, in order: (command, search). */
    val reads = CopyOnWriteArrayList<Pair<String, String?>>()

    private val idsInFlight = AtomicInteger()

    /** The most by-id REQs (of more than one id) it was answering at once. */
    val mostIdsAtOnce = AtomicInteger()

    override fun build(
        url: NormalizedRelayUrl,
        out: WebSocketListener,
    ): WebSocket = Connection(out)

    private inner class Connection(
        private val out: WebSocketListener,
    ) : WebSocket {
        @Volatile
        private var open = false

        @Volatile
        private var signedIn = false
        private val sessions = ConcurrentHashMap<String, NegentropyServerSession>()

        /**
         * Every frame to the client, delivered in the order this connection sent it, as one socket
         * delivers a relay's frames. Each used to go out on a coroutine of its own, so the challenge
         * sent at connect could reach the client after the refusal of a REQ that followed it — an
         * order no relay can produce, and one the client, never having seen a challenge, cannot sign
         * in from (a challenge held back 50ms failed two of GatedPeerTest's sign-in premises).
         */
        private val outbox = Channel<suspend () -> Unit>(Channel.UNLIMITED)

        init {
            scope.launch { for (deliver in outbox) deliver() }
        }

        private fun reply(frame: String) {
            outbox.trySend { out.onMessage(frame) }
        }

        override fun needsReconnect() = false

        override fun connect() {
            open = true
            outbox.trySend { out.onOpen(0, false) }
            reply("""["AUTH","challenge-1"]""")
        }

        override fun disconnect() {
            if (open) {
                open = false
                out.onClosed(1000, "")
            }
        }

        /** What this connection may read of [filter], or the refusal. */
        private fun served(filter: JsonObject): Pair<List<Event>?, String?> {
            val waived = SearchQuery.parse(filter["search"]?.jsonPrimitive?.content).includeSpam
            if (authHint && !signedIn) return emptyList<Event>() to null
            if (membersOnly && !signedIn) return null to "auth-required: this relay serves its members only"
            if (gated && !waived && !signedIn) return null to "auth-required: this relay answers through a web of trust. Sign in, or ask with `include:spam`."
            if (gated && !waived) return emptyList<Event>() to null
            return corpus.filter { matches(filter, it) } to null
        }

        override fun send(msg: String): Boolean {
            val frame = Json.parseToJsonElement(msg).jsonArray
            val cmd = frame[0].jsonPrimitive.content
            when (cmd) {
                "AUTH" -> {
                    auths.incrementAndGet()
                    signedIn = true
                    reply("""["OK","${frame[1].jsonObject.getValue("id").jsonPrimitive.content}",true,""]""")
                }

                "REQ" -> {
                    val subId = frame[1].jsonPrimitive.content
                    val filters = frame.drop(2).map { it.jsonObject }
                    filters.forEach { reads += cmd to it["search"]?.jsonPrimitive?.content }
                    val answers = filters.map(::served)
                    answers.firstNotNullOfOrNull { it.second }?.let {
                        reply(JsonArray(listOf(JsonPrimitive("CLOSED"), JsonPrimitive(subId), JsonPrimitive(it))).toString())
                        return true
                    }
                    val byIds = filters.any { (it["ids"]?.jsonArray?.size ?: 0) > 1 }
                    if (byIds && !answersIds) return true
                    scope.launch {
                        if (byIds) {
                            mostIdsAtOnce.accumulateAndGet(idsInFlight.incrementAndGet(), ::maxOf)
                            delay(idsDelayMs)
                            idsInFlight.decrementAndGet()
                        }
                        for (e in answers.flatMap { it.first.orEmpty() }.distinctBy { it.id }) reply("""["EVENT","$subId",${e.toJson()}]""")
                        reply(if (authHint && !signedIn) """["EOSE","$subId",["auth"]]""" else """["EOSE","$subId"]""")
                    }
                }

                "NEG-OPEN" -> {
                    val subId = frame[1].jsonPrimitive.content
                    val filter = frame[2].jsonObject
                    reads += cmd to filter["search"]?.jsonPrimitive?.content
                    val (set, refusal) = served(filter)
                    if (refusal != null) {
                        reply(JsonArray(listOf(JsonPrimitive("NEG-ERR"), JsonPrimitive(subId), JsonPrimitive(refusal))).toString())
                        return true
                    }
                    val session = NegentropyServerSession(subId, set!!.map { IdAndTime(it.createdAt, it.id) })
                    sessions[subId] = session
                    session.processMessage(frame[3].jsonPrimitive.content)?.let { reply("""["NEG-MSG","$subId","${it.message}"]""") }
                }

                "NEG-MSG" -> {
                    val subId = frame[1].jsonPrimitive.content
                    sessions[subId]?.processMessage(frame[2].jsonPrimitive.content)?.let { reply("""["NEG-MSG","$subId","${it.message}"]""") }
                }

                "NEG-CLOSE" -> {
                    sessions.remove(frame[1].jsonPrimitive.content)
                }

                "EVENT" -> {
                    val id =
                        frame[1]
                            .jsonObject
                            .getValue("id")
                            .jsonPrimitive.content
                    if (membersWrite && !signedIn) {
                        reply("""["OK","$id",false,"auth-required: this relay takes events from its members only"]""")
                        return true
                    }
                    published += id
                    reply("""["OK","$id",true,""]""")
                }

                "CLOSE" -> {}
            }
            return true
        }
    }

    companion object {
        /** NIP-01 matching over the fields these tests send. */
        fun matches(
            f: JsonObject,
            e: Event,
        ): Boolean {
            fun strings(key: String) = f[key]?.jsonArray?.map { it.jsonPrimitive.content }
            strings("ids")?.let { if (e.id !in it) return false }
            strings("authors")?.let { if (e.pubKey !in it) return false }
            strings("kinds")?.let { if (e.kind.toString() !in it) return false }
            f["since"]?.jsonPrimitive?.longOrNull?.let { if (e.createdAt < it) return false }
            f["until"]?.jsonPrimitive?.longOrNull?.let { if (e.createdAt > it) return false }
            return true
        }
    }
}
