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
 * The store's slow-read threshold, in milliseconds, or null for none. Shared
 * because both processes open a store and both may serve a pulse page.
 *
 * HONOURED ONLY WHERE THE PAGE WILL SHOW IT. The slow-read ring is the one
 * place the store retains a query string, and a query string is what somebody
 * typed. An operator who set a threshold but left the client sections off
 * would be keeping that log for nobody to read — the worst of both — so this
 * says so on stderr and keeps nothing.
 *
 * A value that does not parse stops the boot rather than falling back: silently
 * keeping no log is exactly what the operator was trying to change.
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
 * The origin the pulse page's NIP-98 tokens are signed against, no trailing
 * slash.
 *
 * AN OPERATOR SETTING, NOT A REQUEST HEADER. The `u` tag is what stops a token
 * spent at this service from being spent at another, and a server that derives
 * the expected url from the `Host` the caller sent has given that up. The
 * default is the loopback address the boot line prints, which is exactly right
 * for the intended deployment — a private port reached through an SSH tunnel,
 * where the browser and the server agree — and wrong behind a reverse proxy,
 * which is why the page is told the expected url in every refusal rather than
 * left to guess it.
 */
fun pulsePublicUrl(
    env: Map<String, String>,
    key: String,
    port: Int,
): String = env[key]?.trim()?.takeIf { it.isNotEmpty() }?.trimEnd('/') ?: "http://localhost:$port"

/**
 * The administrators who may read the pulse document, or a boot that stops.
 *
 * FAILS CLOSED, LOUDLY. "No administrators" and "every reader is an
 * administrator" are one implementation mistake apart, and only one of them is
 * survivable for a document that quotes what people searched for. A deployment
 * that asks for this page without saying who may read it does not get an open
 * page — it does not get a boot.
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
