pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        maven { url = uri("https://jitpack.io") }
    }
}

rootProject.name = "vespa-relay"

// Services over one Vespa store, split so the mirror can be restarted, retuned,
// or OOM without touching the serving relay:
//
//   :relay  — the serving side: NIP-50 relay + NIP-11 doc + web UI (RelayMain)
//   :sync   — the mirror: strfry-style streams from upstream relays (SyncMain);
//             operators know it as "the router", and its package keeps that name
//
// …over two shared modules, which are shared with DIFFERENT audiences. Keeping
// them apart is the whole point, so each states who may be in it:
//
//   :common — only what the SERVING relay also reads: identity, schema deploy,
//             the quartz log floor, fmtDuration, and the serving-pressure model
//             the processes share over HTTP. It must never gain a dependency on
//             quartz's relay CLIENT or on Ktor — the day it does, it has stopped
//             being "what both read" and become the junk drawer.
//   :web    — how a service serves a page: the Ktor scaffolding, the classpath
//             asset cache with its content-derived validators, and the stats
//             document holder. Domain-free by construction — it depends on Ktor
//             and kotlinx.serialization and on nothing of ours.
//   :peers  — how this deployment talks to OTHER relays, shared by the two
//             client-side planes and by neither of the above: the websocket
//             client and its socket budget, Tor, the NIP-66 verdict record,
//             discovery, the config both planes read, and the ingest queue both
//             write through. It sits ABOVE :common and must stay there.
//
// …and the monitor plane is its own module over :peers:
//
//   :monitor — what is out there and how much of it can we use: the alias fold,
//             the consistency gate and the fitness grades, signing kind-30166
//             records the mirror's roster then selects on. It may not depend on
//             :sync; what it still takes from the mirror arrives through
//             MonitorEngine's constructor, which is where that is accounted for.
include(":common")
include(":web")
include(":peers")
include(":monitor")
include(":relay")
include(":sync")
