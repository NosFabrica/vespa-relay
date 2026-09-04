plugins {
    alias(libs.plugins.kotlin.jvm)
    application
}

// JitPack pins are commit hashes and Gradle picks the lexicographically higher one, so force ours.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    api(project(":common"))
    api(project(":peers"))
    // This process hosts the monitor. The dependency must stay one-directional.
    api(project(":monitor"))
    api(project(":web"))
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.okhttp)
    implementation(libs.typesafe.config)
    testImplementation(kotlin("test"))
    testImplementation(libs.kotlinx.serialization.json)
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

application {
    mainClass = "com.nosfabrica.vespa.relay.SyncMainKt"
    applicationName = "vespa-sync"
}

tasks.test {
    useJUnitPlatform()
    // Every property a probe reads must be forwarded here: the forked test JVM does not
    // inherit them, and a probe missing its switch skips itself silently.
    System.getProperty("abortCensusMinutes")?.let { systemProperty("abortCensusMinutes", it) }
    System.getProperty("abortCensusNsec")?.let { systemProperty("abortCensusNsec", it) }
    System.getProperty("abortCensusUrls")?.let { systemProperty("abortCensusUrls", it) }
    System.getProperty("enforceLocalRelay")?.let { systemProperty("enforceLocalRelay", it) }
    System.getProperty("enforceProbe")?.let { systemProperty("enforceProbe", it) }
    System.getProperty("enforceProviderRelay")?.let { systemProperty("enforceProviderRelay", it) }
    System.getProperty("pagesProbe")?.let { systemProperty("pagesProbe", it) }
    System.getProperty("pagesUrl")?.let { systemProperty("pagesUrl", it) }
    System.getProperty("prodScaleDir")?.let { systemProperty("prodScaleDir", it) }
    System.getProperty("prodScaleProbe")?.let { systemProperty("prodScaleProbe", it) }
    System.getProperty("realRelayProbe")?.let { systemProperty("realRelayProbe", it) }
    System.getProperty("reachNoAuth")?.let { systemProperty("reachNoAuth", it) }
    System.getProperty("reachNsec")?.let { systemProperty("reachNsec", it) }
    System.getProperty("reachUrls")?.let { systemProperty("reachUrls", it) }
    System.getProperty("relayReachProbe")?.let { systemProperty("relayReachProbe", it) }
    // Env vars are hidden from the forked JVM the same way.
    System.getenv("WIDTH_RESCUE_VESPA")?.let { environment("WIDTH_RESCUE_VESPA", it) }
    System.getProperty("seed10040")?.let { systemProperty("seed10040", it) }
    System.getProperty("seed10040Url")?.let { systemProperty("seed10040Url", it) }
    System.getProperty("visitPoolProbe")?.let { systemProperty("visitPoolProbe", it) }
}
