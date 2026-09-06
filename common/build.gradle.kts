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
    // The guards in `arch` read the whole checkout rather than run it: the build files, the
    // include order, every source tree (`.kt` is not enough: a stray browser file is one of
    // the things they catch). Undeclared, Gradle calls this task up to date after
    // exactly the change they exist to catch, and the build goes green over the violation.
    inputs
        .files(
            rootProject.file("settings.gradle.kts"),
            rootProject.fileTree(rootProject.projectDir) {
                include("*/build.gradle.kts")
                include("*/src/**")
            },
        ).withPropertyName("checkoutUnderReview")
        .withPathSensitivity(PathSensitivity.RELATIVE)
}
