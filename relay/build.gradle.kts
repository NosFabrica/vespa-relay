plugins {
    alias(libs.plugins.kotlin.jvm)
    // This module IS the serving app: RelayMain is the entrypoint
    // (`./gradlew :relay:run`, or the `installDist` scripts the Docker image
    // runs). The mirror is :sync's own app.
    application
}

// Our quartz pin WINS over the one vespa-eventstore drags in transitively.
//
// Both are JitPack commit hashes, and Gradle resolves a version conflict by
// picking the "higher" string — which for hashes is lexicographic and therefore
// meaningless. Pinning quartz to 6d518adddb while the store carried 79f198c729
// silently resolved to 79f198c729, because '7' > '6', and the build compiled
// against a quartz that did not have the method being added. Nothing warned;
// the pin simply had no effect.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // The relay is Quartz's protocol engine (RelayServerBase) over a vespa-eventstore store.
    // The router lives in :sync and runs as its own process — nothing here dials out.
    api(project(":common"))
    // The page server: routes, the classpath asset cache and its validators, and
    // the stats document holder. api because serveRelay takes an IconedPage and
    // a StatsSnapshot in its signature.
    api(project(":web"))
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    // NIP-86 ban-list state is persisted to a JSON file (RelayStateStore).
    implementation(libs.kotlinx.serialization.json)
    // The Ktor server: serveRelay binds a port over the Netty engine.
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.websockets)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    implementation(libs.ktor.server.netty)
    testImplementation(kotlin("test"))
    // RelayProtocolTest drives the real protocol over InMemoryEventIndex, which is
    // production code in the store's transitively-exposed :vespa engine jar — no
    // test-fixtures dependency needed. RelayInfoTest parses the NIP-11 doc.
    testImplementation(libs.kotlinx.serialization.json)
    // RelayWebAssetsTest drives GET /web/… in-process: the module directory IS
    // the landing page, so its route, its 304 and its content type are asserted
    // rather than eyeballed against a running container.
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.RelayMainKt"
    applicationName = "vespa-relay"
}

// Write the app version (from the version catalog) into a resource so the
// NIP-11 `version` field tracks releases instead of a hand-edited constant
// (mirrors geode's generated BuildConfig.VERSION).
val generatedVersionDir = layout.buildDirectory.dir("generated/version")
val generateVersionProperties =
    tasks.register("generateVersionProperties") {
        val version = libs.versions.app.get()
        val outFile = generatedVersionDir.map { it.file("relay-version.properties") }
        inputs.property("version", version)
        outputs.file(outFile)
        doLast {
            outFile.get().asFile.apply {
                parentFile.mkdirs()
                writeText("version=$version\n")
            }
        }
    }

sourceSets.main {
    resources.srcDir(generatedVersionDir)
}

tasks.named("processResources") {
    dependsOn(generateVersionProperties)
}

tasks.test {
    useJUnitPlatform()
    // Forwarded, not inherited: a system property on the Gradle command line
    // reaches the DAEMON, and the tests run in a forked JVM that never sees it.
    // `SyncCoverageReportProdScaleProbe` charts the two files the router-side
    // probe left in `prodScaleDir`, and stays off unless asked for by name.
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
}
