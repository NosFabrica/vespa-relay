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

import com.vitorpamplona.quartz.nip01Core.signers.NostrSignerSync
import com.vitorpamplona.quartz.nip98HttpAuth.HTTPAuthorizationEvent
import com.vitorpamplona.quartz.nip98HttpAuth.Nip98AuthVerifier
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertIs

/** A used admin token stays used: nothing a non-administrator can sign may make it replayable. */
class AdminGateTest {
    private val admin = NostrSignerSync()
    private val stranger = NostrSignerSync()
    private val origin = "http://pulse.test"

    private fun token(
        url: String,
        by: NostrSignerSync,
    ): String = by.sign(HTTPAuthorizationEvent.build(url, "GET", null, System.currentTimeMillis() / 1000) {}).toAuthToken()

    @Test
    fun `a flood of self-signed tokens does not make a used admin token replayable`(): Unit =
        runBlocking {
            val gate = Nip98AdminGate(setOf(admin.pubKey), origin)
            val url = "$origin/pulse.json"
            val used = token(url, admin)
            assertIs<Admitted.Admin>(gate.admit(used, "GET", url))

            // Distinct urls, so each is a distinct event id, each verified and each remembered.
            repeat(Nip98AuthVerifier.MAX_REPLAY_ENTRIES + 64) { i ->
                val u = "$url?n=$i"
                assertIs<Admitted.NotAdmin>(gate.admit(token(u, stranger), "GET", u))
            }

            assertIs<Admitted.BadCredentials>(gate.admit(used, "GET", url), "a replayed admin token must not be admitted")
        }

    @Test
    fun `a stranger's token is still single-use`(): Unit =
        runBlocking {
            val gate = Nip98AdminGate(setOf(admin.pubKey), origin)
            val url = "$origin/pulse.json"
            val t = token(url, stranger)
            assertIs<Admitted.NotAdmin>(gate.admit(t, "GET", url))
            assertIs<Admitted.BadCredentials>(gate.admit(t, "GET", url))
        }
}
