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
package com.nosfabrica.vespa.relay.web

import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.buildJsonObject
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.put
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

/** A document about one pubkey, served to a page on another origin: the HTTP half, whatever the document says. */
class PubkeyDocumentTest {
    private val key = "a1".repeat(32)

    @Test
    fun `any origin may read it, a non-key is refused, and a starting service says so`() =
        testApplication {
            var source: PubkeyDocuments? = null
            // Mounted as a status site mounts it: its defaults are where cross-origin reads are allowed.
            application {
                installPageDefaults()
                routing { pubkeyDocument("/coverage/{pubkey}", 30, { source }) }
            }

            assertEquals(HttpStatusCode.BadRequest, client.get("/coverage/not-a-key").status)
            assertEquals(HttpStatusCode.ServiceUnavailable, client.get("/coverage/$key").status, "nothing to answer with yet")

            source =
                PubkeyDocuments { pubkey, query ->
                    buildJsonObject {
                        put("pubkey", pubkey)
                        put("relays", query["relay"].orEmpty().joinToString(" "))
                    }
                }
            val res =
                client.get("/coverage/${key.uppercase()}?relay=wss%3A%2F%2Fa.example&relay=wss%3A%2F%2Fb.example") {
                    header(HttpHeaders.Origin, "https://relay.example")
                }
            assertEquals(HttpStatusCode.OK, res.status)
            // Exactly one: a browser refuses `*, *`, which a client without an Origin never sees.
            assertEquals(listOf("*"), res.headers.getAll(HttpHeaders.AccessControlAllowOrigin), "the asking page is another service's")
            assertEquals("public, max-age=30", res.headers[HttpHeaders.CacheControl])
            val body = Json.parseToJsonElement(res.bodyAsText()).jsonObject
            assertEquals(key, body["pubkey"]!!.jsonPrimitive.content, "taken in either case, asked lowercase")
            assertEquals("wss://a.example wss://b.example", body["relays"]!!.jsonPrimitive.content, "the query reaches the source, every value in order")
        }

    @Test
    fun `a pubkey is 64 hex, in either case`() {
        assertEquals(key, pubkeyOf(key.uppercase()))
        for (bad in listOf(null, "", "a1".repeat(31), "g".repeat(64), "${key}0", "npub1abc")) assertNull(pubkeyOf(bad), "$bad")
    }
}
