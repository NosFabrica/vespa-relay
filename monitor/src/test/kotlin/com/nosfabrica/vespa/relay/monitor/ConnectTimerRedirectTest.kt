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
package com.nosfabrica.vespa.relay.monitor

import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.Proxy
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * One call's listener sees every connect the call makes, a followed redirect's included; `rtt-open` is the
 * first, the connect to the url's own address, which the websocket dial shares.
 */
class ConnectTimerRedirectTest {
    @Test
    fun `rtt-open is the first connect of the call, not a redirect's`() {
        var now = 0L
        val timer = RelayDocument.ConnectTimer { now }
        val call = OkHttpClient().newCall(Request.Builder().url("http://relay.example/").build())
        val ip = InetAddress.getByAddress("relay.example", byteArrayOf(10, 0, 0, 1))
        val plain = InetSocketAddress(ip, 80)
        val secure = InetSocketAddress(ip, 443)

        timer.connectStart(call, plain, Proxy.NO_PROXY)
        now = 20_000_000
        timer.connectEnd(call, plain, Proxy.NO_PROXY, Protocol.HTTP_1_1)
        // 301 Location: https://relay.example/ — followed inside the same call.
        now = 30_000_000
        timer.connectStart(call, secure, Proxy.NO_PROXY)
        now = 330_000_000
        timer.connectEnd(call, secure, Proxy.NO_PROXY, Protocol.HTTP_1_1)

        assertEquals(20L, timer.openMs)
    }
}
