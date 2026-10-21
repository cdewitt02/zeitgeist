plugins {
    id("headline-pulse.java-conventions")
    `java-library`
}

dependencies {
    implementation(libs.kafka.clients)
}
