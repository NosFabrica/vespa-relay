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

/**
 * Every `<link rel="icon">` in a page, replaced by one pointing at [icon].
 *
 * Replaced rather than appended, and this is the half that would silently do
 * nothing if it were skipped: the pages hint the built-in SVG FIRST, and Chrome,
 * Firefox and Edge all prefer an SVG icon to an `.ico`. An operator's icon added
 * alongside would lose to ours in every browser except Safari — an override that
 * appears to work only where nobody looks.
 *
 * [icon] null leaves the markup exactly as it is on disk, which is the common
 * case and the one worth keeping byte-identical: with no override the pages are
 * the classpath's own bytes, and the two hints they carry are already this
 * relay's icon.
 *
 * The value is ESCAPED because of where it can come from. `RELAY_ICON` is an
 * operator's own environment, but `changerelayicon` is a NIP-86 RPC — an
 * authenticated admin over the network — and this function is what puts its
 * argument inside an HTML attribute of a page served to everyone.
 */
fun pageWithIcon(
    html: String,
    icon: String?,
): String {
    val url = icon?.takeIf { it.isNotBlank() } ?: return html
    val replacement = """<link rel="icon" href="${escapeAttribute(url)}" />"""
    var first = true
    return ICON_LINK.replace(html) {
        if (first) {
            first = false
            // The captured indentation keeps the substituted line sitting where
            // the pair it replaces sat; the pages are read by people.
            it.groupValues[1] + replacement + it.groupValues[2]
        } else {
            ""
        }
    }
}

/** One `<link rel="icon" …>` line, with its leading indentation and trailing newline. */
private val ICON_LINK = Regex("""([ \t]*)<link rel="icon"[^>]*>(\r?\n?)""")

/** The four characters that can leave an HTML attribute's quotes, or start an entity. */
private fun escapeAttribute(value: String) =
    value
        .replace("&", "&amp;")
        .replace("\"", "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
