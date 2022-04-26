#!/bin/bash

SUBJECT_PREFIX=app-db.public

curl -s localhost:8081/subjects?subjectPrefix=$SUBJECT_PREFIX | jq '.[]' | tr -d '"' | \
while read topic; do
    model_name=$(echo ${topic/#$SUBJECT_PREFIX.} | sed -E 's/-(value|key)$//')

    echo "Generating model $model_name for topic $topic"

    poetry run datamodel-codegen \
        --url http://localhost:8081/subjects/${topic}/versions/latest/schema \
        --output models/generated/${model_name}.py \
        --strip-default-none
done
