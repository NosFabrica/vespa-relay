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

/** The package holding the guards that read the checkout rather than run it. */
val archPackage = "com.nosfabrica.vespa.relay.arch"

tasks.test {
    useJUnitPlatform()
    // This module's own tests turn on this module's sources alone; the guards run beside them
    // in `archTest`, which is what carries the whole checkout as an input.
    filter { excludeTestsMatching("$archPackage.*") }
}

/**
 * The repo-wide guards, in a task of their own. Their inputs are every build file and every
 * source tree, and attaching that to `test` re-ran this module's whole suite whenever any
 * module's browser code changed.
 */
val archTest =
    tasks.register<Test>("archTest") {
        group = "verification"
        description = "The module graph, the package map, the browser-file rule, the probe-switch list."
        testClassesDirs =
            sourceSets.test
                .get()
                .output.classesDirs
        classpath = sourceSets.test.get().runtimeClasspath
        useJUnitPlatform()
        filter { includeTestsMatching("$archPackage.*") }
        // Undeclared, Gradle calls this up to date after exactly the change the guards exist to
        // catch, and the build goes green over the violation. `.kt` alone is not enough: a stray
        // browser file in an engine module is one of the things they catch.
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

tasks.named("check") { dependsOn(archTest) }
