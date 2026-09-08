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
package com.nosfabrica.vespa.relay.pulse

/**
 * The store's slow-read threshold in milliseconds, or null for none. Honoured only where the
 * page will show the client sections, since the ring retains query strings; otherwise it says so
 * on stderr and keeps nothing. A value that does not parse stops the boot rather than falling back.
 */
fun pulseSlowReadMs(
    env: Map<String, String>,
    key: String,
    clientDetail: Boolean,
    detailKey: String,
): Long? {
    val set =
        env[key]?.trim()?.takeIf { it.isNotEmpty() }?.let {
            it.toLongOrNull() ?: error("$key='$it' is not a number of milliseconds. Unset it to keep no slow-read log.")
        } ?: return null
    if (set <= 0) return null
    if (!clientDetail) {
        System.err.println(
            "$key=$set but $detailKey is off — the slow-read log quotes the query, so it is kept only where the page will show it. Nothing is being retained.",
        )
        return null
    }
    return set
}

/**
 * The origin the pulse page's NIP-98 tokens are signed against, no trailing slash. An operator
 * setting, never the request's `Host`, or the `u` tag would stop binding a token to this service.
 * The default is the loopback address: right for an SSH tunnel, wrong behind a reverse proxy.
 */
fun pulsePublicUrl(
    env: Map<String, String>,
    key: String,
    port: Int,
): String = env[key]?.trim()?.takeIf { it.isNotEmpty() }?.trimEnd('/') ?: "http://localhost:$port"

/**
 * Whether this pulse is served to anyone, from [key] — and the interlock that makes that safe.
 *
 * The document has two halves. The operational half (what the store did, what the engine did,
 * who made it work, what the engine has left) names no person. The client-derived half —
 * hotspots and slow reads — names observer lenses and QUOTES SEARCH TERMS, and is added only
 * when `PULSE_CLIENT_DETAIL` is on.
 *
 * Public is therefore allowed, but only for the half that describes machines. Both on is
 * REFUSED at boot rather than quietly serving one of them: "public" and "publishes what people
 * searched for" must never be one forgotten variable apart, and a deployment that set the
 * detail flag months ago would not think to re-read it when someone opens the page up.
 */
fun pulsePublic(
    env: Map<String, String>,
    clientDerived: Boolean,
    key: String = "PULSE_PUBLIC",
    detailKey: String = "PULSE_CLIENT_DETAIL",
): Boolean {
    val public = env[key]?.trim()?.toBooleanStrictOrNull() ?: false
    require(!(public && clientDerived)) {
        "$key and $detailKey are both on — the client-derived half of the pulse names observer lenses and quotes " +
            "what people searched for, and must not be served to anyone who asks. Turn $detailKey off to serve the " +
            "operational half publicly, or turn $key off to keep the whole document behind the administrator gate."
    }
    return public
}

/**
 * The administrators who may read the pulse document, or a boot that stops. "No administrators"
 * and "everyone is an administrator" are one mistake apart, and this document quotes what
 * people searched for — unless [public] says this deployment serves the operational half to
 * everyone, which [pulsePublic] only allows with the client-derived half off.
 */
fun pulseAdmins(
    admins: Set<String>,
    portKey: String,
    adminKey: String = "RELAY_ADMIN_PUBKEYS",
    public: Boolean = false,
): Set<String> =
    admins.ifEmpty {
        if (public) return emptySet()
        error(
            "$portKey is set but $adminKey is empty — the pulse document names the observer lenses and search terms " +
                "driving this relay's load and can quote slow queries, so it is served only to a proven administrator. " +
                "Set $adminKey, or unset $portKey to serve no pulse page.",
        )
    }
