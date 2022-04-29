#!/bin/bash

curl -i -X POST \
     -H "Content-Type: application/json" \
     --data "$(cat ./services/kafka-connect/source_config.json)" \
     localhost:8083/connectors/

echo
