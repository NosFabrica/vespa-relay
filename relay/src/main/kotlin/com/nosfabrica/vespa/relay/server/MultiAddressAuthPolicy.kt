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

import com.vitorpamplona.quartz.nip01Core.relay.commands.toRelay.AuthCmd
import com.vitorpamplona.quartz.nip01Core.relay.normalizer.NormalizedRelayUrl
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.OptionalAuthPolicy
import com.vitorpamplona.quartz.nip01Core.relay.server.policies.PolicyResult
import com.vitorpamplona.quartz.nip40Expiration.isExpired
import com.vitorpamplona.quartz.nip42RelayAuth.tags.RelayTag
import com.vitorpamplona.quartz.utils.TimeUtils

/**
 * NIP-42 for a relay that answers at more than one address — the clearnet url
 * and a `.onion` hidden service in front of the same port.
 *
 * A client signs the address it dialled, and a Tor client has never heard of
 * the clearnet name: its kind-22242 carries `["relay","ws://…onion/"]`. Quartz's
 * [OptionalAuthPolicy] binds ONE url and rejects everything else with
 * "invalid: relay url does not match", which is right for a relay with one
 * address and silently wrong for this one — every AUTH from the hidden service
 * would fail, and on this relay a failed AUTH is not a locked door but a
 * downgrade: reads keep working and lose their web-of-trust ranking lens
 * ([ObserverBackend] takes the observer from the authenticated pubkey). The
 * onion endpoint would look fine and rank nothing.
 *
 * The whole check is re-stated here rather than delegated because the address
 * comparison is one line inside quartz's `accept` and there is no seam to widen
 * from the outside: the challenge is generated per policy instance, so a second
 * instance bound to the second address would test a challenge no client was
 * ever sent. The other three conditions are quartz's, in quartz's order, with
 * quartz's messages — `RelayOnionAuthTest` pins each one, so this drifting from
 * the engine fails the build rather than the deployment.
 *
 * Three deliberate widenings over the original:
 *  - any of [addresses] satisfies the relay tag, not one fixed url;
 *  - EVERY `relay` tag is considered, not just the first. Quartz's own
 *    `RelayAuthEvent.create(relays, …)` builds multi-relay auth events, and a
 *    client that names both of our addresses means both. This is not a replay
 *    hole: [challenge] is 32 random chars minted per connection, so an event
 *    listing ten relays is still only usable on the connection that issued it;
 *  - a scheme's default port is dropped from both sides before comparing. The
 *    normalizer keeps `ws://host:80/` and `ws://host/` apart, and a hidden
 *    service is published on port 80 — so a client configured with the port
 *    spelled out would sign an address this relay does serve and be told it
 *    does not match. Only the DEFAULT port folds: `ws://host:7777/` is a
 *    different endpoint and still has to be one we answer at.
 */
class MultiAddressAuthPolicy(
    primary: NormalizedRelayUrl,
    alsoAt: Set<NormalizedRelayUrl> = emptySet(),
) : OptionalAuthPolicy(primary) {
    /** [primary] is always accepted; the set is never empty, whatever [alsoAt] holds. */
    private val addresses = (alsoAt + primary).mapTo(HashSet(), NormalizedRelayUrl::withoutDefaultPort)

    override fun accept(cmd: AuthCmd): PolicyResult<AuthCmd> {
        val event = cmd.event

        if (event.isExpired()) {
            return PolicyResult.Rejected("invalid: auth event expired")
        }

        if (!TimeUtils.withinTenMinutes(event.createdAt)) {
            return PolicyResult.Rejected("invalid: created_at is too far from the current time")
        }

        if (event.challenge() != challenge) {
            return PolicyResult.Rejected("invalid: challenge does not match")
        }

        if (event.tags.mapNotNull(RelayTag::parse).none { it.withoutDefaultPort() in addresses }) {
            return PolicyResult.Rejected("invalid: relay url does not match")
        }

        return PolicyResult.Accepted(cmd)
    }
}

/**
 * `ws://host:80/` and `ws://host/` name one endpoint; `wss://host:443/` and
 * `wss://host/` likewise. Quartz's normalizer folds case and adds the trailing
 * slash but keeps an explicitly-spelled default port, so the two spellings are
 * different strings and `==` says they are different relays.
 *
 * Bracketed IPv6 survives it: `[::1]:80` ends with the port and loses it,
 * `[::1]` does not end with `:80` at all — the port only ever follows the
 * closing bracket.
 */
private fun NormalizedRelayUrl.withoutDefaultPort(): NormalizedRelayUrl {
    val port =
        when {
            url.startsWith("ws://") -> ":80"
            url.startsWith("wss://") -> ":443"
            else -> return this
        }
    val authorityStart = url.indexOf("://") + 3
    val slash = url.indexOf('/', authorityStart)
    val authorityEnd = if (slash < 0) url.length else slash
    val portStart = authorityEnd - port.length
    if (portStart <= authorityStart || !url.regionMatches(portStart, port, 0, port.length)) return this
    return NormalizedRelayUrl(url.removeRange(portStart, authorityEnd))
}
