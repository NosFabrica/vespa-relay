// What every module of this repo is: Kotlin on JVM 21, tested on JUnit, on our quartz pin.
plugins {
    id("org.jetbrains.kotlin.jvm")
}

val libs = the<VersionCatalogsExtension>().named("libs")

// JitPack pins are commit hashes and Gradle picks the lexicographically higher one, so force ours.
configurations.all {
    resolutionStrategy {
        force(libs.findLibrary("quartz").get())
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
