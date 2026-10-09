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

/**
 * Whether a failure may be published as "this relay is unreachable". Only the connection
 * itself counts (name resolution, routing, refusal, TLS): an unknown failure costs one retry,
 * a wrong record is a false public statement about someone else's server.
 */
object Unreachability {
    fun proves(e: Exception): Boolean =
        when {
            ourSide(e) -> false

            // Only a resolver that answered "no such name": a bare or temporary failure proves nothing.
            e is java.net.UnknownHostException -> NO_SUCH_NAME.any { it in e.message.orEmpty().lowercase() }

            e is java.net.ConnectException ||
                e is java.net.NoRouteToHostException ||
                e is java.net.PortUnreachableException ||
                e is javax.net.ssl.SSLHandshakeException -> true

            else -> false
        }

    /**
     * A failure whose words put it on this box: a resolver that could not answer (EAI_AGAIN), or
     * no route out of our own network.
     */
    fun ourSide(e: Exception): Boolean {
        val said = e.message?.lowercase() ?: return false
        return OUR_SIDE.any { it in said }
    }

    private val OUR_SIDE = listOf("temporary failure in name resolution", "try again", "network is unreachable")

    /** glibc's, macOS's and Windows' words for a name the resolver knows does not exist. */
    private val NO_SUCH_NAME = listOf("name or service not known", "no address associated with hostname", "nodename nor servname provided", "no such host is known")
}
