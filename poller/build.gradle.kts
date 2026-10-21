plugins {
    id("headline-pulse.java-conventions")
    application
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.clients)
    implementation(libs.jackson.core)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
}

application {
    mainClass = "headlinepulse.poller.Main"
    applicationDefaultJvmArgs = listOf("-Xmx512m")
}
