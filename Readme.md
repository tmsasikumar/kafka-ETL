# Kafka ETL example

This repository contains sample code for ETL using kafka steams

#Must Haves
Kafka, Zookeeper to be installed


# Setup

* ./gradlew build
* cp build/libs/kafka-streams-1.0-SNAPSHOT.jar ~/softwares/kafka_2.13-2.7.0/libs/
* ~/softwares/kafka_2.13-2.7.0/bin/kafka-run-class.sh com.spike.kafkasteam.KafkaETL



* The KafkaTl.java file has the core logic to listen to topic named Events
* It filters events based on a particular action
* Aggregates events based on userid and action and published to aggregatedEvents queue
* you can use this for any ETL in kafka

Data to Publish

part1_prd1_prodview:{"participationId": "part1", "productId": "prd1", "action": "VIDEO_LANDED", "duration": 1 }

