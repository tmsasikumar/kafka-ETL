#!/usr/bin/env bash
bin/kafka-server-start.sh config/server.properties


bin/zookeeper-server-start.sh config/zookeeper.properties


bin/kafka-console-producer.sh --broker-list localhost:9092 --topic bank_master_7 --property "parse.key=true" --property "key.separator=:"

bin/kafka-console-producer.sh --broker-list localhost:9092 --topic transactions_7


kaf consume transaction_output
