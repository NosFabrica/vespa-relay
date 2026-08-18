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

/*
 * ONE icon for this relay, in both places it is asked for. (A block comment and
 * not a KDoc: a toplevel KDoc that documents no declaration is what ktlint calls
 * a dangling one, and it fails the build.)
 *
 * A relay is pictured twice and used to answer differently each time: NIP-11's
 * `icon` (mirrored into the relay's own kind 0 as `picture`) is what a Nostr
 * client draws beside the relay's name, and the favicon is what a browser puts
 * on the tab. They are the same question, so `RELAY_ICON` now answers both and
 * the answer runs in both directions:
 *
 *   - **Set** — the operator's url is what the pages link and what
 *     `/favicon.ico` redirects to, as well as what NIP-11 publishes. A relay
 *     that rebrands rebrands everywhere.
 *   - **Unset** — NIP-11 publishes [selfIconUrl], this relay's own
 *     `/favicon.ico`, so a stock deployment advertises the very icon its tab
 *     shows instead of advertising nothing.
 *
 * The two directions meet in the middle, and the middle is where the trap is:
 * once unset publishes our own url, the doc's `icon` is no longer a reliable
 * signal that an override exists — redirecting `/favicon.ico` to whatever the
 * doc says would send it to itself. That is why [selfIconUrl] is compared
 * against rather than assumed absent; see `iconOverride` in :web.
 */

/**
 * The absolute url of the icon THIS relay serves, or null when we cannot honestly
 * name one.
 *
 * Derived from `RELAY_URL` because there is nowhere else to derive it from: the
 * NIP-11 doc names no host (it is served at whatever address you reached it by)
 * and the relay's kind 0 is published to other relays entirely, where a relative
 * path means nothing.
 *
 * **Refused for anything a stranger cannot reach**, which is the whole reason
 * this is not a one-line concatenation. The compose default is
 * `ws://localhost:7777`, so a plain concatenation would sign
 * `http://localhost:7777/favicon.ico` into a public, replaceable kind 0 on every
 * development boot — a claim about an address that resolves to the READER's own
 * machine. `wss://` is taken as the mark of a real deployment (it is what a
 * clearnet relay behind TLS looks like) and `.onion` is admitted on either
 * scheme, since a hidden service is reachable by name and by nothing else. A
 * plain-http clearnet relay gets null and publishes no icon, exactly as today.
 *
 * The ORIGIN, not the url: `wss://host/alpha` is a relay path, and the icon is
 * served at the root by [favicon] whatever path the websocket answers at.
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
