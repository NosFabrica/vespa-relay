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
 * Replaced rather than appended: browsers prefer the SVG the pages hint
 * first. Null leaves the markup byte-identical. The value is escaped because
 * `changerelayicon` is a network rpc and this puts its argument in an attribute.
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
            // The captured indentation keeps the line where the pair it replaces sat.
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
