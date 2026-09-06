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

import com.nosfabrica.vespa.relay.server.config.defaultRelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import com.vitorpamplona.quartz.nip11RelayInfo.relayInformation
import java.util.Properties

private const val SOFTWARE = "https://github.com/NosFabrica/vespa-relay"

/** The NIPs this relay implements. NIP-86 is appended by the composition root only with an admin key. */
val BASE_SUPPORTED_NIPS = listOf(1, 9, 11, 40, 42, 45, 50, 62, 77)

/**
 * The NIP-50 `nip50` list, in the `"<class> <token>"` spelling. Only tokens the store parses
 * belong here; `observer` and `include:spam` are how an unauthenticated client gets an answer.
 */
val NIP50_FEATURES =
    listOf(
        "ext observer",
        "ext include:spam",
        "ext sort",
        "ext filter:rank",
        "query negate",
        "query exact-phrase-match",
    )

private object RelayInfoMarker

/** The build's version from `relay-version.properties`, or `"dev"` off the classpath. */
val BUILD_VERSION: String by lazy {
    RelayInfoMarker::class.java
        .getResourceAsStream("/relay-version.properties")
        ?.use { stream ->
            Properties().apply { load(stream) }.getProperty("version")
        }?.takeIf { it.isNotBlank() } ?: "dev"
}

/** The NIP-11 document; its `limitation` block is derived from the [RelayLimits] the engine enforces. */
fun buildRelayInfo(
    info: Nip11Info,
    limits: RelayLimits,
    supportedNips: List<Int> = BASE_SUPPORTED_NIPS,
    nip50Features: List<String> = NIP50_FEATURES,
): Nip11RelayInformation =
    relayInformation {
        this.name = info.name
        info.description.ifSet { this.description = it }
        info.icon.ifSet { this.icon = it }
        info.banner.ifSet { this.banner = it }
        info.contactPubkey.ifSet { pubkey = it }
        info.selfPubkey.ifSet { self = it }
        info.contact.ifSet { this.contact = it }
        info.postingPolicy.ifSet { this.postingPolicy = it }
        info.privacyPolicy.ifSet { this.privacyPolicy = it }
        info.termsOfService.ifSet { this.termsOfService = it }
        software = SOFTWARE
        version = info.version ?: BUILD_VERSION
        supports(*supportedNips.toIntArray())
        nip50Features(*nip50Features.toTypedArray())
        limitation(limits)
    }

/** The NIP-11 document as JSON, for the simple case and tests. */
fun relayInfoJson(
    name: String = "vespa-relay",
    description: String? = null,
    icon: String? = null,
    contactPubkey: String? = null,
    selfPubkey: String? = null,
    contact: String? = null,
    version: String? = null,
    limits: RelayLimits = defaultRelayLimits(),
    supportedNips: List<Int> = BASE_SUPPORTED_NIPS,
): String =
    buildRelayInfo(
        info =
            Nip11Info(
                name = name,
                description = description,
                icon = icon,
                contactPubkey = contactPubkey,
                selfPubkey = selfPubkey,
                contact = contact,
                version = version,
            ),
        limits = limits,
        supportedNips = supportedNips,
    ).toJson()

/** Runs [set] with this string only when it is present and non-blank. */
internal inline fun String?.ifSet(set: (String) -> Unit) = this?.takeIf(String::isNotBlank)?.let(set)
