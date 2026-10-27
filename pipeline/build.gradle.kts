plugins {
    id("headline-pulse.java-conventions")
    application
}

dependencies {
    implementation(project(":common"))
    implementation(libs.kafka.streams)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)
    testImplementation(libs.kafka.streams.test.utils)
}

application {
    mainClass = "headlinepulse.pipeline.Main"
    applicationDefaultJvmArgs = listOf("-Xmx1g")
}
