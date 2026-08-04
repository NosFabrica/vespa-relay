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

// Two processes over one Vespa store, split so the mirror can be restarted,
// retuned, or OOM without touching the serving relay:
//
//   :relay  — the serving side: NIP-50 relay + NIP-11 doc + web UI (RelayMain)
//   :sync   — the mirror: strfry-style streams from upstream relays (SyncMain);
//             operators know it as "the router", and its package keeps that name
//   :common — only what both genuinely read: identity, schema deploy, the
//             quartz log floor, fmtDuration, and the serving-pressure model
//             the two processes share over HTTP
include(":common")
include(":relay")
include(":sync")
