plugins {
    alias(libs.plugins.kotlin.jvm)
}

configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // Never :sync. What the monitor needs from the mirror arrives through MonitorEngine's constructor.
    api(project(":peers"))
    implementation(libs.kotlinx.coroutines)
    implementation(libs.kotlinx.serialization.json)
    api(project(":web"))
    testImplementation(kotlin("test"))
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.serialization.json)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    // Every property a probe reads must be forwarded here: the forked test JVM does not
    // inherit them, and a probe missing its switch skips itself silently.
    System.getProperty("authGatedProbe")?.let { systemProperty("authGatedProbe", it) }
    System.getProperty("authGatedUrl")?.let { systemProperty("authGatedUrl", it) }
    System.getProperty("authRefusalCensus")?.let { systemProperty("authRefusalCensus", it) }
    System.getProperty("authRefusalProbe")?.let { systemProperty("authRefusalProbe", it) }
    System.getProperty("authRefusalUrls")?.let { systemProperty("authRefusalUrls", it) }
    System.getProperty("complianceProbe")?.let { systemProperty("complianceProbe", it) }
    System.getProperty("complianceUrls")?.let { systemProperty("complianceUrls", it) }
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
