plugins {
    // Downloads a JDK 21 for the toolchain if one isn't installed.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "headline-pulse"

include("common", "poller", "tools")
