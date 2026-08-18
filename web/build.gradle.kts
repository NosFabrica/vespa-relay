plugins {
    alias(libs.plugins.kotlin.jvm)
}

dependencies {
    // Ktor and nothing else. This module deliberately does NOT depend on
    // :common: the layering rule is that :common holds what the SERVING relay
    // reads, and a page server has no business in that set — the dependency
    // would drag Ktor and Netty into the one module every other module sees.
    //
    // api, not implementation: every route this module hands out is written in
    // Ktor's own types (`Route`, `ApplicationCall`, `ContentType`), so a
    // consumer that mounts one compiles against Ktor whether it declares it or
    // not.
    api(libs.ktor.server.core)
    api(libs.ktor.server.netty)
    implementation(libs.ktor.server.cors)
    implementation(libs.ktor.server.compression)
    // StatsSnapshot merges two writers' halves of one JSON document.
    api(libs.kotlinx.serialization.json)
    testImplementation(kotlin("test"))
    // The asset and page routes are asserted in-process, over the real
    // classpath, rather than eyeballed against a running container.
    testImplementation(libs.ktor.server.test.host)
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}
