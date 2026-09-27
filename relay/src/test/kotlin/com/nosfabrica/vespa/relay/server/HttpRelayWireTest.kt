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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The HTTP commands' small decisions: which body is which command, which frame is which status, who takes gzip. */
class HttpRelayWireTest {
    private fun frame(
        command: HttpCommand,
        body: String,
    ) = parseBody(body.encodeToByteArray())?.let(command::frame)

    @Test
    fun `each command takes its own arguments and nothing else`() {
        assertEquals("""["REQ","http",{"kinds":[1]}]""", frame(HttpCommand.REQ, """{"kinds":[1]}"""))
        assertEquals("""["COUNT","http",{"kinds":[1]},{"ids":[]}]""", frame(HttpCommand.COUNT, """[{"kinds":[1]},{"ids":[]}]"""))
        assertEquals("""["EVENT",{"id":"x"}]""", frame(HttpCommand.EVENT, """{"id":"x"}"""))
        for (bad in listOf("", "[]", "[1]", """[{"kinds":[1]},"x"]""", "null", "\"REQ\"", "{", """{"a":1} trailing""")) {
            assertNull(frame(HttpCommand.REQ, bad), "'$bad' is not filters")
        }
        for (bad in listOf("[]", """[{"id":"x"}]""", "1")) assertNull(frame(HttpCommand.EVENT, bad), "'$bad' is not one event")
    }

    @Test
    fun `each command's answer ends on its own frames`() {
        assertTrue(HttpCommand.REQ.ends("""["EOSE"]"""))
        assertFalse(HttpCommand.REQ.ends("""["EVENT",{}]"""))
        assertTrue(HttpCommand.EVENT.ends("""["OK","x",true,""]"""))
        assertFalse(HttpCommand.EVENT.ends("""["EOSE"]"""))
        assertTrue(HttpCommand.COUNT.ends("""["NOTICE","too big"]"""))
    }

    @Test
    fun `the first frame decides the status`() {
        assertEquals(HttpStatusCode.OK, statusOf("""["EVENT",{}]"""))
        assertEquals(HttpStatusCode.OK, statusOf("""["OK","x",true,"duplicate: have it"]"""))
        assertEquals(HttpStatusCode.BadRequest, statusOf("""["OK","x",false,"invalid: bad signature"]"""))
        assertEquals(HttpStatusCode.Forbidden, statusOf("""["OK","x",false,"blocked: banned"]"""))
        assertEquals(HttpStatusCode.Unauthorized, statusOf("""["CLOSED","auth-required: sign"]"""))
        assertEquals(HttpStatusCode.BadRequest, statusOf("""["NOTICE","error: could not parse message"]"""))
    }

    @Test
    fun `a refusal's machine-readable prefix picks the status`() {
        assertEquals(HttpStatusCode.Unauthorized, statusFor("auth-required: sign in"))
        assertEquals(HttpStatusCode.Forbidden, statusFor("restricted: not for you"))
        assertEquals(HttpStatusCode.Forbidden, statusFor("blocked: banned"))
        assertEquals(HttpStatusCode.TooManyRequests, statusFor("rate-limited: slow down"))
        assertEquals(HttpStatusCode.InternalServerError, statusFor("error: store failed"))
        assertEquals(HttpStatusCode.BadRequest, statusFor("invalid: too many filters"))
        assertEquals(HttpStatusCode.BadRequest, statusFor("no prefix at all"))
    }

    @Test
    fun `answers leave without their subscription id`() {
        assertEquals("""["EVENT",{"id":"x"}]""", withoutSubId("""["EVENT","http",{"id":"x"}]"""))
        assertEquals("""["EOSE"]""", withoutSubId("""["EOSE","http"]"""))
        assertEquals("""["CLOSED","error: x"]""", withoutSubId("""["CLOSED","http","error: x"]"""))
        assertEquals("""["COUNT",{"count":2}]""", withoutSubId("""["COUNT","http",{"count":2}]"""))
        // Frames without a subscription id pass untouched, even when their text happens to read "http".
        assertEquals("""["NOTICE","http"]""", withoutSubId("""["NOTICE","http"]"""))
        assertEquals("""["OK","abc",true,""]""", withoutSubId("""["OK","abc",true,""]"""))
    }

    @Test
    fun `gzip is taken when named or wildcarded, and not at q=0`() {
        assertTrue(acceptsGzip("gzip"))
        assertTrue(acceptsGzip("br, gzip;q=0.5, deflate"))
        assertTrue(acceptsGzip("*"))
        assertTrue(acceptsGzip("GZIP"))
        assertFalse(acceptsGzip(null))
        assertFalse(acceptsGzip("deflate, br"))
        assertFalse(acceptsGzip("gzip;q=0"))
        assertFalse(acceptsGzip("gzip; Q=0.0"))
        assertFalse(acceptsGzip("gzip;q=0, *"), "a named coding decides over the wildcard")
        assertFalse(acceptsGzip("*;q=0"))
    }

    @Test
    fun `an address block matches its own addresses and never resolves a name`() {
        val block = Cidr.parse("172.16.0.0/12")!!
        assertTrue(block.contains("172.20.1.2"))
        assertFalse(block.contains("172.32.0.1"))
        assertTrue(Cidr.parse("10.0.0.1")!!.contains("10.0.0.1"))
        assertFalse(Cidr.parse("10.0.0.1")!!.contains("10.0.0.2"))
        assertTrue(Cidr.parse("::1/128")!!.contains("0:0:0:0:0:0:0:1"))
        assertFalse(Cidr.parse("::1/128")!!.contains("127.0.0.1"))
        assertTrue(Cidr.parse("0.0.0.0/0")!!.contains("8.8.8.8"))
        for (bad in listOf("example.com", "999.1.1.1", "10.0.0.0/33", "10.0.0/8", "", "10.0.0.0/x")) assertNull(Cidr.parse(bad), "'$bad'")
        assertFalse(block.contains("localhost"), "a name is not an address")
    }

    @Test
    fun `a CLOSED frame escapes its reason`() {
        assertEquals("""["CLOSED","error: a \"quoted\" reason"]""", closedFrame("error: a \"quoted\" reason"))
    }
}
