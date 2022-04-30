#!/bin/bash

usage() {
    printf "\nUsage: %s [-h] [-t TOPIC] [-o OFFSET]\n\n" $(basename $0)
    printf "Helper wrapper around kafka-consumer-groups.sh to reset offsets for transformer\n\n"
    printf "Options:\n"
    printf "\t%-20s   %-80s\n" "-h" 		        "This help info"
    printf "\t%-20s   %-80s\n" "-t TOPIC"   		"Topic name to reset (defaults to all topics)"
    printf "\t%-20s   %-80s\n" "-o OFFSET"  		"Offset value to reset to (defaults to earliest)"
    printf "\n"
}


while getopts ':ht:o:' OPTION; do
    case $OPTION in
        t)
            TOPIC=$OPTARG
            ;;
        o)
            OFFSET=$OPTARG
            ;;
        h | ?)
            usage >&2
            exit $([[ $OPTION == "h" ]] && echo 0 || echo 2)
            ;;
    esac
done

shift $(($OPTIND - 1))

DOCKER_COMMAND=""
DOCKER_COMMAND+="docker-compose exec kafka kafka-consumer-groups.sh"
DOCKER_COMMAND+=" --bootstrap-server localhost:9092"
DOCKER_COMMAND+=" --group transformer"
DOCKER_COMMAND+=$([[ $TOPIC ]] && echo " --topic $TOPIC" || echo " --all-topics")
DOCKER_COMMAND+=" --reset-offsets"
DOCKER_COMMAND+=$([[ $OFFSET ]] && echo " --to-offset $OFFSET" || echo " --to-earliest")
DOCKER_COMMAND+=" --execute"

bash -c "$DOCKER_COMMAND"
