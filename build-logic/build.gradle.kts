plugins {
    `kotlin-dsl`
}

dependencies {
    implementation(libs.plugins.kotlin.jvm.map { "${it.pluginId}:${it.pluginId}.gradle.plugin:${it.version}" })
}
