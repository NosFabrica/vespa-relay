import org.gradle.api.tasks.testing.Test

/**
 * Hands each named `-D` switch on the Gradle command line to the forked test JVM, which does not
 * inherit them; a probe missing its switch skips itself. ProbeSwitchesAreForwardedTest checks the list.
 */
fun Test.forwardProbeSwitches(vararg names: String) {
    names.forEach { name -> System.getProperty(name)?.let { systemProperty(name, it) } }
}

/** The same for environment variables, which a reused daemon would otherwise keep from the test JVM. */
fun Test.forwardProbeEnv(vararg names: String) {
    names.forEach { name -> System.getenv(name)?.let { environment(name, it) } }
}
