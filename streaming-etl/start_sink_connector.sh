#!/bin/bash

curl -i -X POST \
     -H "Content-Type: application/json" \
     --data "$(cat ./services/kafka-connect/sink_config.json)" \
     localhost:8083/connectors/

echo
