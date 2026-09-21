#!/bin/bash


cd /opt/kafka/bin/
sleep 10

#./kafka-topics.sh  --if-not-exists --create --topic ml-output --partitions 3 --replication-factor 1 --config cleanup.policy=delete --config retention.bytes=536780912 --config retention.ms=1800000 --bootstrap-servers localhost:9091

IFS=','

for TOPIC in $KAFKA_TOPICS; do 

    echo "creating topic ${TOPIC}"
    ./kafka-topics.sh --if-not-exists \
    --create \
    --topic ${TOPIC} \
    --partitions ${KAFKA_NUM_OF_PARTITIONS}\
    --replication-factor ${KAFKA_NUM_OF_REPLICAS}\
    --config cleanup.policy=delete \
    --config retention.bytes=536780912 \
    --config retention.ms=1800000 \
    --bootstrap-server ${KAFKA_BOOTSTRAP_SERVERS}

done

echo "the topics has been created"

wait