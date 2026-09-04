plugins {
    alias(libs.plugins.kotlin.jvm)
}

configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // :peers AND NOTHING OF :sync. That is the whole point of the module: the
    // monitor measures relays and signs NIP-66 records, and everything it needs
    // to do that is the shared peer vocabulary. What it still takes from the
    // mirror — the ingest queue a probe dial's events land in, the socket
    // refcount, the pinned urls — arrives through its constructor, which is
    // where MonitorEngine's KDoc accounts for it.
    api(project(":peers"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    // This plane serves its own status page: the four pass rows and the per-url
    // verdicts they sign. api because MonitorStatus's document is published
    // through a StatsSnapshot.
    api(project(":web"))
    testImplementation(kotlin("test"))
    // The page's routes and every module it imports are asserted in-process,
    // the same way the relay's and the mirror's are.
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    // Forwarded, not inherited: a system property on the Gradle command line
    // reaches the DAEMON, and the tests run in a forked JVM that never sees it.
    // These dial the public internet or write to a relay, so they stay off unless asked for by name.
    //
    // EVERY property a probe in this module reads has to appear here. A missing
    // one does not fail — the probe skips itself with its own "[skip]" line, which
    // reads exactly like a probe that was never asked for.
    System.getProperty("authGatedProbe")?.let { systemProperty("authGatedProbe", it) }
    System.getProperty("authGatedUrl")?.let { systemProperty("authGatedUrl", it) }
    System.getProperty("authRefusalCensus")?.let { systemProperty("authRefusalCensus", it) }
    System.getProperty("authRefusalProbe")?.let { systemProperty("authRefusalProbe", it) }
    System.getProperty("authRefusalUrls")?.let { systemProperty("authRefusalUrls", it) }
    System.getProperty("complianceProbe")?.let { systemProperty("complianceProbe", it) }
    System.getProperty("complianceUrls")?.let { systemProperty("complianceUrls", it) }
    System.getProperty("emptyFilterProbe")?.let { systemProperty("emptyFilterProbe", it) }
    System.getProperty("emptyFilterUrls")?.let { systemProperty("emptyFilterUrls", it) }
    System.getProperty("liveBudget")?.let { systemProperty("liveBudget", it) }
    System.getProperty("writeOrderTsv")?.let { systemProperty("writeOrderTsv", it) }
    System.getProperty("writeOrderReplayUrls")?.let { systemProperty("writeOrderReplayUrls", it) }
    System.getProperty("liveBudgetUrls")?.let { systemProperty("liveBudgetUrls", it) }
    System.getProperty("liveConsistency")?.let { systemProperty("liveConsistency", it) }
    System.getProperty("liveConsistencyUrls")?.let { systemProperty("liveConsistencyUrls", it) }
    System.getProperty("liveFoldGroups")?.let { systemProperty("liveFoldGroups", it) }
    System.getProperty("liveFoldProbe")?.let { systemProperty("liveFoldProbe", it) }
    System.getProperty("onionFoldProbe")?.let { systemProperty("onionFoldProbe", it) }
    System.getProperty("onionFoldSocks")?.let { systemProperty("onionFoldSocks", it) }
    System.getProperty("onionFoldUrls")?.let { systemProperty("onionFoldUrls", it) }
    System.getProperty("seedVerdicts")?.let { systemProperty("seedVerdicts", it) }
    System.getProperty("seedVerdictsCount")?.let { systemProperty("seedVerdictsCount", it) }
    System.getProperty("seedVerdictsLegacy")?.let { systemProperty("seedVerdictsLegacy", it) }
    System.getProperty("seedVerdictsNsec")?.let { systemProperty("seedVerdictsNsec", it) }
    System.getProperty("seedVerdictsUrl")?.let { systemProperty("seedVerdictsUrl", it) }
    System.getProperty("selfConsistency")?.let { systemProperty("selfConsistency", it) }
    System.getProperty("selfConsistencyUrls")?.let { systemProperty("selfConsistencyUrls", it) }
}
