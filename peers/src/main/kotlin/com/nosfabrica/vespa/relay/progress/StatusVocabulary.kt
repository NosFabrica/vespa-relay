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
package com.nosfabrica.vespa.relay.progress

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.jsonPrimitive

/** Which status document a definition is written for. */
enum class StatusPlane {
    MIRROR,
    MONITOR,
}

/**
 * What every number in the status documents means, shipped inside the document so the reader
 * of the JSON has the definition beside the value. One entry per published member and no
 * entry without one; `StatusVocabularyTest` pins both directions.
 */
object StatusVocabulary {
    /**
     * Every member either status document can publish, defined. A name each plane defines in its own
     * words reads both here; a document gets its own plane's through [termsFor].
     */
    val TERMS: JsonObject = union()

    private val byPlane: Map<StatusPlane, JsonObject> =
        mapOf(
            StatusPlane.MIRROR to JsonObject(SHARED_TERMS + MIRROR_TERMS),
            StatusPlane.MONITOR to JsonObject(SHARED_TERMS + MONITOR_TERMS),
        )

    /** The definitions [document] needs: every member it publishes, at any depth, and no others. */
    fun termsFor(document: JsonObject): JsonObject = select(document, TERMS)

    /** [termsFor] for [plane]'s own document: the shared terms and that plane's, never the other plane's words. */
    fun termsFor(
        document: JsonObject,
        plane: StatusPlane,
    ): JsonObject = select(document, byPlane.getValue(plane))

    private fun select(
        document: JsonObject,
        terms: JsonObject,
    ): JsonObject {
        val published = LinkedHashSet<String>()

        fun walk(o: JsonObject) {
            for ((member, value) in o) {
                published += member
                when (value) {
                    is JsonArray -> value.filterIsInstance<JsonObject>().forEach(::walk)
                    is JsonObject -> walk(value)
                    else -> Unit
                }
            }
        }
        walk(document)
        return JsonObject(terms.filterKeys { it in published })
    }

    /** The three sections as one map. A shared name is shared only; a per-plane pair reads as both definitions. */
    private fun union(): JsonObject {
        val clash = SHARED_TERMS.keys intersect (MIRROR_TERMS.keys + MONITOR_TERMS.keys)
        check(clash.isEmpty()) { "the glossary defines $clash as shared AND per plane; one or the other" }
        val both = MIRROR_TERMS.keys intersect MONITOR_TERMS.keys
        val paired =
            both.associateWith {
                JsonPrimitive(
                    "On the mirror's document: ${MIRROR_TERMS.getValue(it).jsonPrimitive.content} " +
                        "On the monitor's: ${MONITOR_TERMS.getValue(it).jsonPrimitive.content}",
                )
            }
        return JsonObject(SHARED_TERMS + MIRROR_TERMS + MONITOR_TERMS + paired)
    }
}

/** One glossary section: a name defined twice in it is refused, since a lookup by name would keep only the last. */
internal fun glossary(define: Glossary.() -> Unit): JsonObject = JsonObject(Glossary().apply(define).terms)

internal class Glossary {
    val terms = LinkedHashMap<String, JsonPrimitive>()

    fun term(
        name: String,
        definition: String,
    ) = check(terms.put(name, JsonPrimitive(definition)) == null) {
        "the glossary defines `$name` twice; one entry must cover every place it is published"
    }
}
