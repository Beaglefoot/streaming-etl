#!/bin/bash

docker-compose exec kafka kafka-topics.sh \
    --bootstrap-server=kafka:9092 \
    --delete \
    --topic $1
