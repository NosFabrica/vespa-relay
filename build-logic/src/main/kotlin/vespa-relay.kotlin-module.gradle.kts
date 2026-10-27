// What every module of this repo is: Kotlin on JVM 21, tested on JUnit, on our quartz pin.
plugins {
    id("org.jetbrains.kotlin.jvm")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// Quartz is a Maven Central release (com.vitorpamplona.quartz). Forced, so no transitive request
// can move it; and JitPack's coordinates for it (com.github.vitorpamplona.quartz, what the store
// at a pin built before 2.4.0 names) are SUBSTITUTED by it. They are a different group, so without
// the substitution the graph would hold both modules — two copies of every quartz class, which no
// version conflict resolution notices — rather than one quartz.
configurations.all {
    resolutionStrategy {
        val quartz = libs.findLibrary("quartz").get().get()
        force(quartz)
        dependencySubstitution {
            for (artifact in listOf("quartz", "quartz-jvm")) {
                substitute(module("com.github.vitorpamplona.quartz:$artifact"))
                    .using(module("com.vitorpamplona.quartz:$artifact:${quartz.version}"))
                    .because("quartz comes from Maven Central; JitPack's group is the same library")
            }
        }
    }
}

dependencies {
    "testImplementation"(kotlin("test"))
}

kotlin {
    jvmToolchain(21)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
    // The benches need a few GB of heap; unit tests must not. Opt in with -PtestHeap=6g.
    providers.gradleProperty("testHeap").orNull?.let { maxHeapSize = it }
}
