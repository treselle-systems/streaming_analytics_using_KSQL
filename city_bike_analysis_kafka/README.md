# KAFKA PRODUCER AND CONSUMER

Produces the station details and trip details to kafka topic, Which is enriched by Kafka SQL by Confluent before consumed.

## Getting started

 - Clone the project from GitHub
 - See the build command section for building the project
 - Jar file is created under target folder
 - Execute the project to produce and consume as mentioned in execution command section

## Prerequisties

 - Scala
 - Kafka
 - SBT
 - JDK

## Build command
 - sbt clean
 - sbt assembly

## Execution command

 - Producer: java -cp kafka_producer_consumer.jar com.treselle.kafka.core.Producer topic localhost:9092 producer_type
 - Consumer: java -cp kafka_producer_consumer.jar com.treselle.kafka.core.Consumer topic localhost:9092