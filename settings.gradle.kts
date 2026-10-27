pluginManagement {
    // The shared module conventions (vespa-relay.kotlin-module) and the probe-switch helpers.
    includeBuild("build-logic")
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
        // JitPack serves the store and nothing else, so a coordinate missing upstream cannot be
        // substituted from it; the group is spelled both ways because JitPack caches per spelling.
        // Quartz left it at 2.4.0 for Maven Central (build-logic maps JitPack's quartz group there).
        exclusiveContent {
            forRepository { maven { url = uri("https://jitpack.io") } }
            filter {
                includeGroupByRegex("""com\.github\.(NosFabrica|nosfabrica)\.vespa-eventstore""")
            }
        }
    }
}

rootProject.name = "vespa-relay"

// Two processes over one store, so the mirror can restart without the relay
// noticing, over shared modules with distinct audiences. See AGENTS.md, Layout.
//
//   :common  — what the serving relay also reads. Never quartz's relay client, never Ktor.
//   :web     — the browser files and the Ktor scaffolding; the seam is /stats.json.
//   :peers   — how this deployment talks to other relays; above :common.
//   :monitor — the alias fold, the consistency gate and the fitness grades; never depends on :sync.
//   :relay   — the serving side (RelayMain).
//   :sync    — the mirror, which hosts the monitor (SyncMain).
include(":common")
include(":web")
include(":peers")
include(":monitor")
include(":relay")
include(":sync")
