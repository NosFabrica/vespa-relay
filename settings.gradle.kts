pluginManagement {
    // The shared module conventions (vespa-relay.kotlin-module) and the probe-switch helpers.
    includeBuild("build-logic")
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

// Quartz (com.vitorpamplona.quartz) and its crypto-kmp (com.vitorpamplona.crypto),
// libschnorr256k1-kotlin (com.vitorpamplona.schnorr256k1) and negentropy-kmp
// (com.vitorpamplona.negentropy) are released on Nostr + Blossom only: Magpie serves those groups,
// trusting only the release key vitorpamplona.com names as `releases`, and gradle/magpie.lock pins
// that key and every resolved version's files, as in quartz and the store.
plugins {
    id("com.vitorpamplona.magpie") version "0.2.3"
}

magpie {
    group("com.vitorpamplona.quartz")
    group("com.vitorpamplona.crypto")
    group("com.vitorpamplona.schnorr256k1")
    group("com.vitorpamplona.negentropy")
}

dependencyResolutionManagement {
    repositories {
        google()
        mavenCentral()
        // JitPack serves the store and quartz's commit pins and nothing else, so a coordinate
        // missing upstream cannot be substituted from it; the store's group is spelled both ways
        // because JitPack caches per spelling. Whichever way quartz is pinned, its other group is
        // mapped onto the pin (build-logic), so the graph holds one quartz.
        exclusiveContent {
            forRepository { maven { url = uri("https://jitpack.io") } }
            filter {
                includeGroupByRegex("""com\.github\.(NosFabrica|nosfabrica)\.vespa-eventstore""")
                includeGroup("com.github.vitorpamplona.quartz")
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

// Local checkouts in place of the pins, to build against an upstream change before it is released:
//   ./gradlew -PvespaEventStoreDir=../vespa-eventstore :relay:compileKotlin
//   ./gradlew -PquartzDir=../quartz :relay:compileKotlin
providers.gradleProperty("vespaEventStoreDir").orNull?.let { dir ->
    includeBuild(dir) {
        dependencySubstitution {
            for (group in listOf("com.github.NosFabrica.vespa-eventstore", "com.github.nosfabrica.vespa-eventstore")) {
                substitute(module("$group:store")).using(project(":store"))
                substitute(module("$group:engine")).using(project(":engine"))
            }
        }
    }
}
providers.gradleProperty("quartzDir").orNull?.let { dir ->
    includeBuild(dir) {
        dependencySubstitution {
            for (group in listOf("com.vitorpamplona.quartz", "com.github.vitorpamplona.quartz")) {
                substitute(module("$group:quartz")).using(project(":quartz"))
                substitute(module("$group:quartz-jvm")).using(project(":quartz"))
            }
        }
    }
}
