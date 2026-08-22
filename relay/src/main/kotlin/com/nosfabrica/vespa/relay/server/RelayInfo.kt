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

import com.nosfabrica.vespa.relay.config.defaultRelayLimits
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import com.vitorpamplona.quartz.nip11RelayInfo.relayInformation
import java.util.Properties

private const val SOFTWARE = "https://github.com/NosFabrica/vespa-relay"

/**
 * The NIPs this relay actually implements. NIP-86 is not here: the composition
 * root appends it only when an admin key is configured, so the doc never
 * claims an admin API that would reject every request.
 */
val BASE_SUPPORTED_NIPS = listOf(1, 9, 11, 40, 42, 45, 50, 62, 77)

/**
 * The NIP-50 `nip50` list: the search extensions this relay honors, in the
 * `"<class> <token>"` spelling relays already publish (`ext include:spam`,
 * `query negate`).
 *
 * It matters more here than on a relay that merely searches. Since
 * [com.nosfabrica.vespa.relay.server.LensRequiredPolicy], `observer:` and
 * `include:spam` are not garnish on a query — they are the two ways an
 * unauthenticated client gets an answer at all, and NIP-11 is the one place a
 * client can learn that BEFORE it is refused. Only tokens the store actually
 * parses belong here; the doc is a promise, and a token listed but ignored is
 * worse than one left out.
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

/**
 * The relay's version, read from the build-generated `relay-version.properties`
 * so NIP-11 `version` tracks releases instead of a hand-edited constant.
 * `"dev"` when the resource isn't on the classpath.
 */
val BUILD_VERSION: String by lazy {
    RelayInfoMarker::class.java
        .getResourceAsStream("/relay-version.properties")
        ?.use { stream ->
            Properties().apply { load(stream) }.getProperty("version")
        }?.takeIf { it.isNotBlank() } ?: "dev"
}

/**
 * Build the NIP-11 relay information document. The `limitation` block is
 * derived from the same [RelayLimits] the engine enforces. The object (rather
 * than only its JSON) is returned so NIP-86 admin RPCs can update it at
 * runtime.
 */
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
        info.contactPubkey.ifSet { pubkey = it } // admin contact key
        info.selfPubkey.ifSet { self = it } // the relay's OWN key
        info.contact.ifSet { this.contact = it } // human contact (email / uri)
        info.postingPolicy.ifSet { this.postingPolicy = it }
        info.privacyPolicy.ifSet { this.privacyPolicy = it }
        info.termsOfService.ifSet { this.termsOfService = it }
        software = SOFTWARE
        version = info.version ?: BUILD_VERSION
        supports(*supportedNips.toIntArray())
        nip50Features(*nip50Features.toTypedArray())
        limitation(limits)
    }

/** The NIP-11 document as JSON — a convenience for the simple case and tests. */
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

/** Run [set] with this string only when it's present and non-blank. */
internal inline fun String?.ifSet(set: (String) -> Unit) = this?.takeIf(String::isNotBlank)?.let(set)
