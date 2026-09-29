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
package com.nosfabrica.vespa.relay.graph

import java.io.File

/** Who may run read-only Cypher against the graph: nobody, relay admins, any NIP-98 signer, anyone. */
enum class CypherAccess { OFF, ADMIN, AUTH, PUBLIC }

/**
 * The Neo4j graph projection's settings (docs/configuration.md, "Graph projection"). Off unless
 * `GRAPH_PROJECTION=on`; when on, a missing password or an unknown value stops the boot.
 */
data class GraphSettings(
    val url: String,
    val user: String,
    val password: String,
    val database: String,
    val queueCapacity: Int,
    val cursorFile: File,
    val reconcileEverySeconds: Long,
    val cypher: CypherAccess,
    val excludedKinds: Set<Int>,
    val tagNodes: Set<String>?,
) {
    companion object {
        /** Null when the projection is off. */
        fun fromEnv(env: Map<String, String>): GraphSettings? {
            val on = env["GRAPH_PROJECTION"]?.trim()?.lowercase()
            when (on) {
                null, "", "off", "false" -> return null
                "on", "true" -> Unit
                else -> error("GRAPH_PROJECTION='$on' is not on or off")
            }

            // At least 1: a queue of 0 would drop everything, and a tick of 0 would spin on Vespa.
            fun num(
                key: String,
                default: Long,
            ): Long {
                val value = env[key]?.trim()?.takeIf { it.isNotEmpty() }?.let { it.toLongOrNull() ?: error("$key='$it' is not a number") } ?: default
                require(value >= 1) { "$key=$value must be at least 1" }
                return value
            }
            return GraphSettings(
                url = env["NEO4J_URL"]?.trim()?.takeIf { it.isNotEmpty() } ?: "bolt://neo4j:7687",
                user = env["NEO4J_USER"]?.trim()?.takeIf { it.isNotEmpty() } ?: "neo4j",
                password = env["NEO4J_PASSWORD"]?.takeIf { it.isNotEmpty() } ?: error("GRAPH_PROJECTION=on needs NEO4J_PASSWORD"),
                database = env["NEO4J_DATABASE"]?.trim()?.takeIf { it.isNotEmpty() } ?: "neo4j",
                queueCapacity = num("GRAPH_QUEUE", 100_000).coerceAtMost(Int.MAX_VALUE.toLong()).toInt(),
                cursorFile = File(env["GRAPH_CURSOR_FILE"]?.trim()?.takeIf { it.isNotEmpty() } ?: "/var/lib/vespa-relay/graph-cursor.txt"),
                reconcileEverySeconds = num("GRAPH_RECONCILE_SECONDS", 300),
                cypher =
                    env["GRAPH_CYPHER"]?.trim()?.takeIf { it.isNotEmpty() }?.let { v ->
                        CypherAccess.entries.firstOrNull { it.name.equals(v, ignoreCase = true) } ?: error("GRAPH_CYPHER='$v' is not off, admin, auth or public")
                    } ?: CypherAccess.OFF,
                excludedKinds =
                    env["GRAPH_EXCLUDE_KINDS"]
                        ?.split(',')
                        ?.mapNotNull { it.trim().takeIf { t -> t.isNotEmpty() } }
                        ?.map {
                            it.toIntOrNull() ?: error("GRAPH_EXCLUDE_KINDS has '$it', not a kind")
                        }?.toSet() ?: emptySet(),
                // Blank is UNSET (the default list), as for every other key: compose passes
                // `${GRAPH_TAG_NODES:-}`, and reading that as "no tag nodes" silently dropped them all.
                tagNodes =
                    env["GRAPH_TAG_NODES"]
                        ?.takeIf { it.isNotBlank() }
                        ?.split(',')
                        ?.map { it.trim() }
                        ?.filter { it.isNotEmpty() }
                        ?.toSet(),
            )
        }
    }
}
