plugins {
    alias(libs.plugins.kotlin.jvm)
}

// JitPack pins are commit hashes and Gradle picks the lexicographically higher one, so force ours.
configurations.all {
    resolutionStrategy {
        force(libs.quartz)
    }
}

dependencies {
    // api: both appear in this module's signatures.
    api(libs.quartz)
    api(libs.vespa.eventstore.store)
    // The pulse document is built here, above the store and below Ktor: :web
    // owns the route and deliberately does not depend on this module.
    api(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
