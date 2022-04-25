#!/bin/bash

docker-compose exec kafka kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 \
    --group transformer \
    --all-topics \
    --reset-offsets \
    --to-earliest \
    --execute
