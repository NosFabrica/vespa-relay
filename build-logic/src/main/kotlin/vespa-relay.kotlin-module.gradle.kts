// What every module of this repo is: Kotlin on JVM 21, tested on JUnit, on our quartz pin.
plugins {
    id("org.jetbrains.kotlin.jvm")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// Quartz is pinned once, in libs.versions.toml: a release through Magpie (com.vitorpamplona.quartz)
// or a commit JitPack builds (com.github.vitorpamplona.quartz). Forced, so no transitive request can
// move it; and the OTHER group's coordinates for it (what a store pinned the other way names) are
// SUBSTITUTED by it. They are different groups, so without the substitution the graph would hold
// both modules — two copies of every quartz class, which no version conflict resolution notices —
// rather than one quartz.
configurations.all {
    resolutionStrategy {
        val quartz = libs.findLibrary("quartz").get().get()
        force(quartz)
        val other = listOf("com.vitorpamplona.quartz", "com.github.vitorpamplona.quartz").single { it != quartz.group }
        dependencySubstitution {
            for (artifact in listOf("quartz", "quartz-jvm")) {
                substitute(module("$other:$artifact"))
                    .using(module("${quartz.group}:$artifact:${quartz.version}"))
                    .because("quartz is pinned once; $other is the same library")
            }
        }
    }
}

dependencies {
    "testImplementation"(kotlin("test"))
}

kotlin {
    jvmToolchain(25)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    // The benches need a few GB of heap; unit tests must not. Opt in with -PtestHeap=6g.
    providers.gradleProperty("testHeap").orNull?.let { maxHeapSize = it }
    // A failure prints its assertion message and stack, not just the exception's class and the
    // line Kotlin maps a lambda to: CI keeps no test report, so the console is all a flake leaves.
    testLogging {
        events(org.gradle.api.tasks.testing.logging.TestLogEvent.FAILED)
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}
