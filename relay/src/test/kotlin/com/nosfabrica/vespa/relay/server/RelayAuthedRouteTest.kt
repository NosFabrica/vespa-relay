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

import io.ktor.client.request.get
import io.ktor.client.request.header
import io.ktor.client.statement.bodyAsText
import io.ktor.http.HttpHeaders
import io.ktor.http.HttpStatusCode
import io.ktor.server.routing.routing
import io.ktor.server.testing.testApplication
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * `GET /authed` — the one document this relay serves that is about its clients.
 *
 * Every other endpoint here is public on purpose and each of their KDocs says
 * why: they describe stored events or an aggregate latency, and name nobody.
 * This one is a list of the people currently reading, so what is asserted is
 * mostly what it REFUSES — no route without a token, no body without the right
 * one, and no cache anywhere along the way.
 */
class RelayAuthedRouteTest {
    private val alice = "a".repeat(64)

    private fun readers() = AuthedReaders(TOKEN).apply { signedIn(1, alice) }

    @Test
    fun `the route does not exist at all without a token`() =
        testApplication {
            // Not "responds 401": absent. An operator who has not opted in has
            // no endpoint to leave open by mistake, and nothing is tracked.
            application { routing { authedReaders(null) } }

            assertEquals(HttpStatusCode.NotFound, client.get("/authed").status)
        }

    @Test
    fun `a request with no credential is refused`() =
        testApplication {
            application { routing { authedReaders(readers()) } }

            val response = client.get("/authed")

            assertEquals(HttpStatusCode.Unauthorized, response.status)
            assertFalse(response.bodyAsText().contains(alice), "a refused request learns nothing about who is here")
            assertEquals("Bearer", response.headers[HttpHeaders.WWWAuthenticate])
        }

    @Test
    fun `a wrong credential is refused in the same words as a missing one`() =
        testApplication {
            application { routing { authedReaders(readers()) } }

            val missing = client.get("/authed")
            val wrong = client.get("/authed") { header(HttpHeaders.Authorization, "Bearer nope") }

            assertEquals(missing.status, wrong.status)
            assertEquals(
                missing.bodyAsText(),
                wrong.bodyAsText(),
                "\"your header was the right shape\" is worth nothing to the sync process and something to everyone else",
            )
        }

    @Test
    fun `the token serves who is signed in`() =
        testApplication {
            application { routing { authedReaders(readers()) } }

            val response = client.get("/authed") { header(HttpHeaders.Authorization, "Bearer $TOKEN") }

            assertEquals(HttpStatusCode.OK, response.status)
            assertEquals("""{"pubkeys":["$alice"],"count":1,"omitted":0}""", response.bodyAsText())
        }

    @Test
    fun `it is never cached`() =
        testApplication {
            // The other documents here carry an ETag and `no-cache` because they
            // are large and change rarely. This one changes on every login and
            // is small, and a copy of who was signed in sitting in an
            // intermediary is worth more than the round trip it saves.
            application { routing { authedReaders(readers()) } }

            val response = client.get("/authed") { header(HttpHeaders.Authorization, "Bearer $TOKEN") }

            assertEquals("no-store", response.headers[HttpHeaders.CacheControl])
            assertTrue(response.headers[HttpHeaders.ETag] == null, "an ETag would invite a conditional request for this")
        }

    private companion object {
        const val TOKEN = "a-shared-secret"
    }
}
