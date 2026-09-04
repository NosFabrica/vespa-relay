# Fitness test decisions

What the monitor's fitness, reachability and NIP-66 fact tests used to carry
in their comments and no other decisions file records. The pass, the silence
table, the anchor and the tag layout are in `fitness.md`, `peers.md`,
`aliases.md` and `monitor-engine.md`.

**A hang-up mid-transfer and our own exceptions never prove unreachable.**
`wss://nip85.nosfabrica.com` answered the handshake in 50ms and then threw
`EOFException` part-way through a large page, and a
`ConcurrentModificationException` inside the fan-out cost another relay a
signed `unreachable` record in a real cycle. `Unreachability.proves` accepts
only failures that show the connection never opened; a read timeout or an
unknown throwable stays quiet, which costs one retry next cycle instead of a
false record under our signature.

**`AuthGatedFetchProbe` pins our wiring, not quartz's auth path.** Against
`auth.nostr1.com` all three arms (explicit true, explicit false, derived)
returned the same 91 events with `done=eose`: the relay never challenged, so
the run proves only that `hasAuthResponder()` is true because
`RelayAuthenticator` registers itself, which is the value the router had been
hardcoding. The 1485ms on the first arm is the websocket connect the later
arms reuse, not a cost of the flag. Quartz's own `Nip42AuthGatedFetchTest`
is the one that drives a relay that gates.
