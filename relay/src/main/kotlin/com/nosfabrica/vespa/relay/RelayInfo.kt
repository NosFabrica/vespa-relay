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
package com.nosfabrica.vespa.relay

import com.vitorpamplona.quartz.nip01Core.relay.server.policies.RelayLimits
import com.vitorpamplona.quartz.nip11RelayInfo.Nip11RelayInformation
import com.vitorpamplona.quartz.nip11RelayInfo.relayInformation
import java.util.Properties

private const val SOFTWARE = "https://github.com/NosFabrica/vespa-relay"

/**
 * The NIPs this relay actually implements, in the order they are advertised.
 * NIP-86 (relay management) is not here: it is appended by the composition
 * root only when an admin key is configured, so the doc never claims an admin
 * API that would reject every request.
 */
val BASE_SUPPORTED_NIPS = listOf(1, 9, 11, 40, 42, 45, 50, 62, 77)

private object RelayInfoMarker

/**
 * The relay's own version, sourced from the build (the `app` version in the
 * Gradle version catalog, written into `relay-version.properties` by the
 * `generateVersionProperties` task) so the NIP-11 `version` tracks releases
 * instead of drifting from a hand-edited constant. Falls back to `"dev"` when
 * the generated resource isn't on the classpath (e.g. running from raw
 * sources without the generating task).
 */
val BUILD_VERSION: String by lazy {
    RelayInfoMarker::class.java
        .getResourceAsStream("/relay-version.properties")
        ?.use { stream ->
            Properties().apply { load(stream) }.getProperty("version")
        }?.takeIf { it.isNotBlank() } ?: "dev"
}

/**
 * Build the NIP-11 relay information document object. Identity comes from the
 * composition root's config; the technical fields describe what
 * [NostrRelayServer] actually implements. The `limitation` block is derived
 * from the same [RelayLimits] the engine enforces, so what the doc advertises
 * and what the relay rejects can never disagree.
 *
 * The object (rather than only its JSON) is returned so the composition root
 * can hold it mutably and let NIP-86 admin RPCs (change name/description/icon)
 * update it at runtime.
 */
fun buildRelayInfo(
    info: Nip11Info,
    limits: RelayLimits,
    supportedNips: List<Int> = BASE_SUPPORTED_NIPS,
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
        // The limitation block IS the enforced RelayLimits, not a duplicate.
        limitation(limits)
    }

/**
 * The NIP-11 relay information document as JSON (served on `GET /` with
 * Accept: application/nostr+json). Kept as a convenience over [buildRelayInfo]
 * for the simple case and for tests.
 */
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
