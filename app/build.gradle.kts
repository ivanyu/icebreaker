plugins {
    application
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("com.formdev", "flatlaf", "3.5.4")
    implementation("org.apache.iceberg", "iceberg-core", "1.7.1")

    testImplementation(libs.junit.jupiter)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

application {
    mainClass = "me.ivanyu.icebreaker.App"
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
