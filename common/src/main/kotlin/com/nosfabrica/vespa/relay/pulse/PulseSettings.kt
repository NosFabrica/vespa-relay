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
 * The administrators who may read the pulse document, or a boot that stops. "No administrators"
 * and "everyone is an administrator" are one mistake apart, and this document quotes what
 * people searched for.
 */
fun pulseAdmins(
    admins: Set<String>,
    portKey: String,
    adminKey: String = "RELAY_ADMIN_PUBKEYS",
): Set<String> =
    admins.ifEmpty {
        error(
            "$portKey is set but $adminKey is empty — the pulse document names the observer lenses and search terms " +
                "driving this relay's load and can quote slow queries, so it is served only to a proven administrator. " +
                "Set $adminKey, or unset $portKey to serve no pulse page.",
        )
    }
