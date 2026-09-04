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
package com.nosfabrica.vespa.relay.peers

import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl

/**
 * The stream's socket refcount. Quartz never closes a connection it opened,
 * so whoever dials a url claims it first and releases it after; the socket
 * closes when the last holder lets go. Claim before the dial, or the probe
 * decrements somebody else's count.
 */
interface Sockets {
    /** Take a share of this url's socket before dialling it. */
    fun claim(url: NormalizedRelayUrl)

    /** Give it back, closing the socket if nothing else holds one. */
    fun release(url: NormalizedRelayUrl)

    companion object {
        /** Leaves every socket where it is: a leaked connection is recoverable, one closed under a live transfer is not. */
        val NONE =
            object : Sockets {
                override fun claim(url: NormalizedRelayUrl) = Unit

                override fun release(url: NormalizedRelayUrl) = Unit
            }
    }
}
