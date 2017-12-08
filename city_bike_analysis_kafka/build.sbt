name := "city_bike_analysis_kafka"
version := "1.0"
scalaVersion := "2.11.8"

assemblyJarName in assembly := "city_bike_analysis_kafka.jar"

libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.11.0.1"

// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.9.1"

// https://mvnrepository.com/artifact/com.thoughtworks.paranamer/paranamer
libraryDependencies += "com.thoughtworks.paranamer" % "paranamer" % "2.3"