# Relay server decisions

The history behind `relay/.../RelayMain.kt`, the `server/` package (HttpServer,
NostrRelayServer, the NIP-42 and lens policies, TrustNotice, SearchGate,
BanListFile, RelayIcon, Nip86Route, RelayWebSocket, HttpRelayRoutes, HttpRelayGate) and `config/`
(RelayAddresses, EnvSettings, PubKeys), moved out of the source so the code
reads on its own. One paragraph per decision; `git log -L` on the function
finds the commit.

**A sync setting on the relay refuses to boot.** The mirror moved to its own
process, and a `SYNC_CONFIG` aimed at the relay would once have started it and
now starts nothing. A configured component must never be silently inert, and
"the mirror stopped mirroring" is the worst spelling of it. The rest of the
`SYNC_*`/`ROUTER_*`/`PARSE_AUDIT_*` family only warns, because none of those
starts a subsystem, but `PARSE_AUDIT_FILE` left here would look exactly like an
audit that found nothing. `SYNC_MANIFEST_FILE` is exempt because the stats
rollup really does read it off the shared volume; the other three sync files
(`SYNC_STATE_FILE`, `SYNC_SWEEP_STATE_FILE`, `SYNC_PROGRESS_FILE`) are served by
the sync process's own status site now.

**Maintenance runs behind the server, awaited nowhere.** Blocking the port on
any of it turns every restart into an outage; the trust reconcile alone was
measured at over twelve minutes. The shutdown hook cancels the scope and does
not wait: an unfinished reconcile costs a less complete ranking until the next
start, which is what it costs anyway.

**The stats rollup has two intervals.** The corpus-wide groupings and the cheap
counters (totals, freshness, trust health, the sync heartbeat) are not within an
order of magnitude of each other in cost, and the counters were being served
fifteen minutes stale for no reason. One rollup, two timers, because the tiers
write disjoint halves of the document. A stats interval that does not parse
stops the boot: `=0s` and `=off` are the obvious spellings of "turn this off",
and `?: default` accepted both by running the rollup on the schedule the
operator was trying to change.

**The totals and the trust chain moved to the charts pass.** The per-minute
tier assumed a `count()` was cheap because it materialises nothing, and that
kind 30382 ran to thousands of events. Both were wrong: a `count()` over `true`
visits every document, and the live store held 28.7M scores, so every minute
walked the whole corpus once and every score twice (the provider grouping and
the score count), with a count over the whole reputation store beside them. The members
kept their places in the document, since the page reads them there. What it
cost is `newestEvent` at the charts cadence rather than the minute; it is now
read off the histogram's spans for free, and the page's staleness line is a day.

**`RELAY_ICON` answers both NIP-11 and the favicon, in both directions.** A
relay was pictured twice and answered differently each time. Once unset
publishes the relay's own `/favicon.ico` url, the doc's `icon` is no longer a
signal that an override exists, so redirecting `/favicon.ico` to whatever the
doc says would send it to itself; the server therefore compares against
`selfIconUrl` rather than assuming it absent. That url is refused for anything
a stranger cannot reach, because the compose default `ws://localhost:7777`
would otherwise sign `http://localhost:7777/favicon.ico` into a public
replaceable kind 0 on every development boot.

**The NIP-11 doc is the kind 0's source.** A NIP-86 rename republishes the
relay's profile through the same `RelayProfile` instance that published at
boot, and the fields the doc no longer carries are cleared from the kind 0 for
the same reason an unset `RELAY_DESCRIPTION` is. Before the hook, the kind 0
kept saying what the environment said at boot while `GET /` said something
else.

**`Onion-Location` is a response hook, not a route.** Amethyst records the
header from any response, including the websocket handshake, and dials the
`.onion` instead when Tor is on; the clients that most need it may only ever
open the websocket. A request that already arrived on the `.onion` gets none,
or a host would be cached as its own alternative. The value is `http://`, not
`ws://`, because readers parse it with an http url parser (okhttp's
`toHttpUrl()` returns null for a ws scheme and drops the advertisement without
a word).

**The hidden-service address is watched by mtime, once per second.**
`File.lastModified()` costs about a microsecond on a missing path, while the
`Files.readString` in a `runCatching` it replaced cost about 37µs because a
missing path builds a `NoSuchFileException` with a stack trace, and missing is
the default (compose sets the path on every relay). The interval keeps the
cost independent of traffic now that the header rides every response.
Watching rather than reading once is also what makes a rotated `.onion` land
without a restart. `nextLook` is seeded from the clock rather than
`Long.MIN_VALUE`: `now - Long.MIN_VALUE` overflows negative, so the first look
was always skipped and an address Tor had already published stayed invisible
until the second ask. Two tests caught it.

**NIP-42 is restated, not delegated.** Quartz's `OptionalAuthPolicy` binds one
url, and the address comparison is one line inside its `accept` with no seam
to widen; a second instance bound to the second address would test a
challenge no client was sent. On this relay a failed AUTH is a lost ranking
lens rather than a locked door, so every Tor client would have looked fine and
ranked nothing. The default port folds because a hidden service is published
on port 80 and the normalizer keeps `ws://host:80/` and `ws://host/` apart.
`RelayOnionAuthTest` pins each of quartz's conditions so drift fails the build.

**The login notice is paid once per identity per connection.** An AUTH frame
stays valid for its ten-minute window against the challenge that minted it,
so a client may resend it any number of times and each would start another
walk of the store on a scope the socket's close does not cancel. The
`authorize` hook must not block (the client waits on the `OK`) and must not
throw (quartz records a throw as a failed login).

**Reads declare their lens.** The store applies a web-of-trust lens as a
filter and has no house observer (one would gate anonymous visitors to the
sliver of the corpus it has scored, measured at about 0.1%), so a read with no
lens is the whole corpus with trust off, and that must be asked for rather
than got by saying nothing. Measured on staging on 2026-08-22: a filter with
`include:spam` returned the same five ids as the bare filter, and one with
`observer:` returned five different ones, so the waiver costs a compliant
client nothing but the token. `limitation.auth_required` stays false because
both tokens work on a socket that signs nothing. NIP-77 is gated through the
same hook because quartz builds a `ReqCmd` out of a NEG-OPEN's filters;
`NegentropyGatedTest` pins it, because the day that stops is the day a
reconcile becomes the one unguarded read of the corpus's id space. The cost is
that an anonymous peer must send `include:spam` to mirror from here; our own
router reads the refusal as `VisitPool.refusedOutright`.

**`observer:` must be 64 hex to count as declared.** The store drops anything
else, so `observer:npub1…` would pass the gate and rank nothing, the silent
no-lens read the gate exists to stop.

**The trust notice says one thing.** The two links are a chain: with no kind
10040 there is no service to ask about, so "we hold none of your provider's
scores" is the same finding restated as a guess. Every `30382:rank` service is
considered, not the first: reading only the first told a reader whose second
provider was fully mirrored that their scores were missing, on every login,
forever. A failed store read says nothing, because "we do not have your
provider list" while Vespa was unreachable is a claim about the reader's own
publishing the relay cannot support.

**One ranked read per connection.** A NIP-50 search for a common word scores
millions of postings across every match thread, so two on one socket share
rather than overlap. Measured on staging on 2026-09-03 (`bitcoin`, kind 1): one
search answered in 3.8s, three at once in 5.0s each, six at once in 6.8s each.
The search page had sent nine (one per keystroke plus the submit plus the
pager's preload) and its first page landed at 9.1s on a relay that answers the
query alone in 3.7s. The cap is per connection, because a global one would put
one reader's slow word in front of everyone's fast one. The permit is held from
the store call to EOSE, not to the end of the call, because a REQ parks at its
live tail. A searching COUNT is gated too; it ran the same match set at about
15x the search it summarizes.

**Pressure is sampled to EOSE, never to the end of the call.** A REQ parks
until the client closes it, so timing the whole call recorded subscription
lifetimes as read latency and pinned the ingest backoff at max. `/pressure`
caps its `samples` field at the gate it answers: uncapped it was a lifetime
query counter, and polling it twice handed anyone the relay's
queries-per-second.

**The reference expansion moved into the store.** It lived here as an
`IEventStore` decorator until store `a9ce0d254c`. The reader's enrolment had
to be cached with a TTL, because a relay cannot see the sync process feeding
10040s into the same index from another JVM. The move did not close that by
itself: the store's pass was invalidated only by its own process's writes, so
the TTL was simply lost until store `226db24694` bounded the pass's age
(`TRUST_PROVIDER_REFRESH_SECONDS`, #243). Placing a subject by the
confidence its pointer expressed needs the pointer's relevance, which
`IEventStore` does not expose. What this relay owns is the budget. A cap
`coerceAtLeast(0)` once turned `-1` into a cap of zero, the feature on and
adding nothing; a negative now stops the boot and zero is honoured as zero.

**An unparseable setting stops the boot.** `REQUIRE_READ_LENS=treu` once
failed closed, and before that a typo silently opened the corpus, which cannot
be noticed from outside. Every number and switch now goes through
`StrictEnv`, so a value outside its range or not a switch names itself at boot
instead of running as a default nobody chose. `SWEEP_ORPHAN_SCORES_ON_START`
is the exception: it deletes data, so anything but exactly `true` is the dry
run rather than a refused boot or, worse, a `1` read as delete.

**Pubkey settings take npubs only.** Bare hex has no checksum, so one mistyped
character is a valid-looking key that is nobody. A bad value throws rather
than being dropped, because an admin who cannot administer or a ban that is
not enforced looks exactly like the feature not working; `DENY_KINDS=4;5`
silently denying nothing is the same failure for kinds. An nsec is refused
outright because quartz would decode it to its public key and leave a private
key in a public setting.

**The ban file's read and decode are guarded together.** The outer catch once
covered only `parseToJsonElement`, and every field accessor throws in its own
right, so a state file that was valid JSON but wrong in shape (a truncated
write, a hand edit) threw out of `RelayMain` and the relay did not start. The
documented behaviour is to say so loudly and come up with empty lists.
`openBanStore` treats blank as unset because compose's `${RELAY_STATE_FILE:-}`
delivers `""`.

**The NIP-86 body is bounded before buffering.** NIP-98 binds the token to the
sha256 of the whole body, so the handler can only check size after reading;
an unbounded receive was a pre-auth OOM vector for anyone who found the port.

**A slow websocket consumer is disconnected, not throttled.** Dropping EVENT or
EOSE frames would corrupt NIP-01 semantics. The polite close frame queues
behind the very congestion that tripped it, so the session is cancelled after
a short grace, which closes the socket and stops its REQs querying for replies
nobody reads. The 30s ping and 60s timeout exist because a phone that walked
off NAT leaves a half-open session whose subscriptions and buffers survive
until the OS gives up, which can be never.

**Over its character budget, only a client that stopped reading is cut off.**
8192 frames of large events let one non-reading client pin hundreds of
megabytes, so the queue has a 16M-character budget. A hard cap on it closed
healthy clients: quartz pushes a whole REQ answer at store speed, and one page
of 5000 large follow or relay lists passes the budget while the client drains
it normally, then retries forever. Past the budget, a connection is closed only
when the writer has waited 30s on one frame, checked on every offer and once a
second; a non-reader holds at most what arrives in that window, still under the
8192-frame bound, and the frame bound is unchanged.

**The websocket's frame cap is the engine's message length, in bytes.** Ktor's
default is unbounded, and it judges a frame from its header, so an anonymous
client could declare a gigabyte and have the server try to hold it before
quartz's character limit ever saw a message. The cap is three bytes per
character, the same bound the HTTP body cap uses, and a refused frame closes
with 1009.

**A sign-in check is skipped, not queued, past `TrustNotice.MAX_IN_FLIGHT`.**
Every AUTH by a fresh key started a store walk on the maintenance scope; a
burst of keys was a burst of walks. The notice is a courtesy, so a saturated
relay sends none rather than holding a queue that grows with the burst.

**The ban list is synced before it is renamed into place, and a failed write is
said rather than thrown.** Without the sync a power loss could leave the rename
pointing at an empty file, which loads as no bans at all. A disk that refuses
the write keeps the previous file and logs; the ban already applies in memory,
and throwing turned an enforced ban into an RPC 500.

**`/kind_stats.html` redirects rather than 404s.** The old url is bookmarked
and printed in this repo's own history, and the answer moved rather than went
away: the Kinds table on `/stats.html` covers every kind where the old page
counted only the ones it knew to name.

**The pulse guard is resolved before the schema deploy, and its window opens
with the store.** `PulseGuard` throws when `PULSE_PORT` is set with no
administrator named, and that refusal belongs beside the other settings checks:
a boot that deploys a schema and opens a store before saying "you forgot
`RELAY_ADMIN_PUBKEYS`" costs two minutes to deliver one line. `storeOpenedAt`
is taken right after `VespaEventStore.open`, not at process start or where the
site is mounted, because the page states every total as cumulative over that
window and a relay that spent two minutes deploying before opening the store
would otherwise claim a window that never held those counters.

**Every setting `.env.example` documents has to reach a container.** Compose
injects only what a service maps, and a documented variable that no service maps
does not fail: it is ignored, and the operator gets a relay that quietly does not
do the thing they configured. It happened twice before `ComposePassesEnvTest`
existed: the NIP-86 ban list was read by the relay and never passed through, so
a `.env` ban was silently unenforced, and the pulse page's port was documented
before it was mapped, so the page never appeared. The test is loose about how a
name reaches compose (an `environment:` mapping, a `ports:` entry, a
`mem_limit`, a volume path) and strict about it appearing at all; its `exempt`
map is short on purpose, because every entry is a claim that setting the
variable under compose should do nothing, and an exemption for a deleted setting
is checked for the same reason.

**An HTTP command is a websocket session that ends at its answer.** The body
is the client frame itself, and the handler hands it to `RelayServerBase.serve`,
the same entry the socket uses, rather than calling the store: every limit,
policy, the raw-frame path and the pressure sample come with it, and a later
quartz change reaches both transports at once. The frame is parsed once, to
refuse what HTTP does not carry (AUTH, CLOSE, NEG-*) and to know what ends the
answer, then run with its text, so the policies that judge the wire text still
see it. The answer is the frames
themselves, one per line, because a client then parses HTTP and the socket
the same way, and the last line says whether the answer finished (`EOSE`) or
was cut (`CLOSED`) — a bare array of events cannot tell a dropped connection
from an empty tail. The status waits for the first frame so a refusal is still
an HTTP error; after that it cannot change.

**NIP-98 signs the session in, after the policy chain votes.** A NIP-42 AUTH
needs a challenge round trip a single request does not have, so quartz's
handler proves the key from the `Authorization` header and records it with
`RelaySession.authenticateByTransport`, which asks every policy's
`acceptTransportIdentity` first. Everything in this relay's stack has no
objection by default except `MultiAddressAuthPolicy`, whose quartz parent
refuses until it opts in: it overrides `authorizeTransport` to accept, and sends
no post-login notice, since over HTTP a NOTICE would end the answer. The key is
then in `authenticatedUsers` exactly as after AUTH, which is what
`LensRequiredPolicy` and `ObserverBackend` read. The token must bind the body's
hash, so it authorizes one command.

**NIP-98 tokens are not single-use over HTTP.** The admin rpc remembers every
token it accepts and refuses a second use; the HTTP commands do not. That
memory is one process's, so behind a load balancer it holds per instance only,
against NIP-FE's premise that any instance answers any request; and the body's
hash already limits a captured token to repeating the command it signs, inside
its 60-second window. What that leaves is re-running a gated read for a minute,
which is what NIP-98 servers generally accept.

**The HTTP route gzips its own stream.** Ktor's Compression plugin holds output
until its deflater buffer fills, which measured as the first line arriving with
the last one; the route suppresses the plugin for its 200 and sync-flushes a
`GZIPOutputStream` at each flush point, so a compressed line is a delivered one.
Flush points are "nothing else waiting", so a store page leaves as one write.

**HTTP commands are capped per address and in all, and refused rather than
queued.** `SearchGate` holds one ranked read per connection, which HTTP defeats
by opening a connection per request. A queued request would hold a socket and
spend its deadline waiting, so the gate answers `429` (the address) or `503`
(the relay) at once. Behind a proxy every client is the proxy, so the address
comes from `HTTP_RELAY_CLIENT_HEADER`, read only when the socket's peer is one
of `HTTP_RELAY_TRUSTED_PROXIES`: from anyone else the header is the client's
own claim, and honouring it let one client take every slot by naming a new
address per request. Its last entry, across every line of it, because that is
the one the proxy wrote. The gate runs before NIP-98 is verified, since the
verifier spends the token and a refused request would otherwise burn it.

**The deadline is checked between frames and never interrupts a write.**
Cancelling a write mid-way could leave half a line, or with gzip send buffered
bytes twice (measured: the stream broke with `invalid block type`). A reader
that has stopped reading is bounded instead by a hard stop five seconds past
the deadline, which drops the connection rather than finishing it.

**No negentropy over HTTP.** It was served for a while as `/neg`, one NIP-77
round per request carrying its filter: the responder keeps no state between
rounds but its sealed snapshot, so nothing needed holding. Removed, as upstream
NIP-FE (nostr-protocol/nips#2484) removed it: a stateless round rebuilds or
re-finds that snapshot every round, on a relay that takes writes between them,
where the websocket builds it once per sync; and a mirror peering over HTTP gains
nothing the socket does not already give it. Reconciliation stays on the socket.

**The gzip stream is handed on at 64 KB, not only at flushes.** The route
flushes when no frame is waiting, which in a burst is never; compressed output
sat in the sink's own buffer and a reader that stopped reading left the whole
answer in the heap (measured: 12 MB accepted with nothing read). Draining
past the threshold puts the gzip path under the socket's backpressure like the
plain one.

**The HTTP commands run on quartz's handler.** They began here, standing in for
what quartz did not expose: frames recognised by their text, a NIP-98 key riding
beside the session in a side table, a body re-serialized into a frame string.
The engine half landed upstream (amethyst #4212, its review fixes #4214): a typed
`SessionSink`, `authenticateByTransport`, and the transport-neutral
`HttpRelayHandler`, which this relay now calls. `HttpRelayRoutes` keeps only the
host's part: the bounded body read, the gate, the headers and the gzip sink.

**One URL, whole frames.** NIP-FE began here as three paths (`/req`, `/count`,
`/event`) taking the command's arguments, with the subscription id stripped from
every answer frame. The published text (NostrHub, kind 30817,
`d=nip-fe-nostr-over-http`) dropped all of it: a request is a POST to the relay's
URL carrying the frame a client would send on the socket, id and all, and the
answer is the socket's frames unchanged. A client reuses its NIP-01 parser as it
is, the relay feeds the body to its socket handler, and nothing is rewritten on
the way out. NIP-86 already answered POSTs to `/`, so the two share the route,
told apart by NIP-86's `application/nostr+json+rpc`; with the commands off, every
POST is the rpc's, as before.

