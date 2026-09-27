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

import io.ktor.http.HttpStatusCode
import kotlinx.serialization.SerializationException
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.booleanOrNull

/**
 * The client commands HTTP carries, one path each. A body is the command's arguments after its
 * subscription id (a lone object where the command takes one), re-serialized here so it can only
 * ever be those arguments; the answer ends on the first frame in [answers].
 */
internal enum class HttpCommand(
    val path: String,
    private val answers: Set<String>,
) {
    REQ("/req", setOf("EOSE", "CLOSED", "NOTICE")),
    COUNT("/count", setOf("COUNT", "CLOSED", "NOTICE")),
    EVENT("/event", setOf("OK", "NOTICE")),
    ;

    /** The client frame [body] stands for, or null when [body] is not this command's arguments. */
    fun frame(body: JsonElement): String? {
        val args =
            when (this) {
                REQ, COUNT -> filters(body)
                EVENT -> (body as? JsonObject)?.let(::listOf)
            } ?: return null
        val head =
            when (this) {
                REQ -> listOf(JsonPrimitive("REQ"), JsonPrimitive(HTTP_SUB_ID))
                COUNT -> listOf(JsonPrimitive("COUNT"), JsonPrimitive(HTTP_SUB_ID))
                EVENT -> listOf(JsonPrimitive("EVENT"))
            }
        return JsonArray(head + args).toString()
    }

    /** Whether [frame] is the last one this command's answer carries. */
    fun ends(frame: String): Boolean = verbOf(frame) in answers
}

/** The subscription id every HTTP command runs under inside the engine; NIP-FE answers carry none. */
internal const val HTTP_SUB_ID = "http"

/** The frames that carry a subscription id in the engine. */
private val SUBSCRIPTION_FRAMES = setOf("EVENT", "EOSE", "CLOSED", "COUNT")

private const val SUB_ID_FIELD = ",\"$HTTP_SUB_ID\""

/**
 * [frame] as NIP-FE sends it, the engine's `"http"` subscription id taken out:
 * `["EVENT","http",{…}]` → `["EVENT",{…}]`, `["EOSE","http"]` → `["EOSE"]`. Other frames pass as they are.
 */
internal fun withoutSubId(frame: String): String {
    val verb = verbOf(frame)
    if (verb !in SUBSCRIPTION_FRAMES) return frame
    val at = verb.length + 3
    if (!frame.startsWith(SUB_ID_FIELD, at)) return frame
    return frame.substring(0, at) + frame.substring(at + SUB_ID_FIELD.length)
}

private fun filters(body: JsonElement): List<JsonElement>? =
    when (body) {
        is JsonObject -> listOf(body)
        is JsonArray -> body.takeIf { it.isNotEmpty() && it.all { f -> f is JsonObject } }
        else -> null
    }

/** The body as JSON, or null when it does not parse. */
internal fun parseBody(body: ByteArray): JsonElement? =
    try {
        Json.parseToJsonElement(body.decodeToString())
    } catch (_: SerializationException) {
        null
    }

/** A relay frame's verb: quartz writes compact JSON, so it is the first string after `[`. */
internal fun verbOf(frame: String): String {
    if (!frame.startsWith("[\"")) return ""
    val end = frame.indexOf('"', 2)
    return if (end < 0) "" else frame.substring(2, end)
}

/**
 * The status [frame] (a NIP-FE frame, subscription id already out) gives an answer that opens with it: an accepting frame is 200 and the answer
 * streams; a refusal's NIP-01 prefix picks the code, and a NOTICE is a command that never ran.
 */
internal fun statusOf(frame: String): HttpStatusCode =
    when (verbOf(frame)) {
        "CLOSED" -> statusFor(stringAt(frame, 1))

        // A duplicate is already stored, which is what the caller asked for, whichever flag the store set.
        "OK" -> if (okAccepted(frame) || stringAt(frame, 3).startsWith("duplicate:")) HttpStatusCode.OK else statusFor(stringAt(frame, 3))

        "NOTICE" -> HttpStatusCode.BadRequest

        else -> HttpStatusCode.OK
    }

/** The HTTP status a NIP-01 machine-readable prefix stands for. */
internal fun statusFor(reason: String): HttpStatusCode =
    when (reason.substringBefore(':', "")) {
        "auth-required" -> HttpStatusCode.Unauthorized
        "restricted", "blocked" -> HttpStatusCode.Forbidden
        "rate-limited" -> HttpStatusCode.TooManyRequests
        "error" -> HttpStatusCode.InternalServerError
        else -> HttpStatusCode.BadRequest
    }

internal fun closedFrame(reason: String): String = JsonArray(listOf(JsonPrimitive("CLOSED"), JsonPrimitive(reason))).toString()

private fun parsedFrame(frame: String): JsonArray? = runCatching { Json.parseToJsonElement(frame) as? JsonArray }.getOrNull()

private fun stringAt(
    frame: String,
    index: Int,
): String = (parsedFrame(frame)?.getOrNull(index) as? JsonPrimitive)?.content.orEmpty()

private fun okAccepted(frame: String): Boolean = (parsedFrame(frame)?.getOrNull(2) as? JsonPrimitive)?.booleanOrNull == true

/**
 * Whether an `Accept-Encoding` admits gzip. A `gzip` entry decides on its own; `*` speaks only for
 * codings the header does not name (RFC 9110 12.5.3).
 */
internal fun acceptsGzip(header: String?): Boolean {
    val weights =
        header
            .orEmpty()
            .split(',')
            .map { it.trim() }
            .filter { it.isNotEmpty() }
            .associate { entry ->
                val parts = entry.split(';').map { it.trim() }
                val q =
                    parts
                        .drop(1)
                        .firstOrNull { it.startsWith("q=", ignoreCase = true) }
                        ?.substring(2)
                        ?.toDoubleOrNull() ?: 1.0
                parts[0].lowercase() to q
            }
    val q = weights["gzip"] ?: weights["*"] ?: return false
    return q > 0
}
