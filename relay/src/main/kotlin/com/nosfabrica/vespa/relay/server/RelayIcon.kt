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

/**
 * The absolute url of the icon this relay serves at `/favicon.ico`, or null when no stranger could
 * reach it: only `wss://` or a `.onion` qualifies, so a `ws://localhost` default is never signed
 * into a public kind 0. The origin only, whatever path the websocket answers at.
 */
internal fun selfIconUrl(relayUrl: String?): String? {
    val url = relayUrl?.trim()?.takeIf { it.isNotBlank() } ?: return null
    val http =
        when {
            url.startsWith("wss://", ignoreCase = true) -> "https://" + url.substring(6)
            url.startsWith("ws://", ignoreCase = true) -> "http://" + url.substring(5)
            url.startsWith("https://", ignoreCase = true) || url.startsWith("http://", ignoreCase = true) -> url
            else -> return null
        }
    val scheme = http.substringBefore("://")
    val authority = http.substringAfter("://").substringBefore('/').ifBlank { return null }
    val host = authority.substringBefore(':')
    val reachable = scheme.equals("https", ignoreCase = true) || host.endsWith(".onion", ignoreCase = true)
    if (!reachable) return null
    return "$scheme://$authority/favicon.ico"
}
