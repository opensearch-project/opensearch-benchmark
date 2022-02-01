#!/usr/bin/env bash

# Execute this script `./stop.sh` to stop the docker containers for leader cluster, follower cluster and metricstore.
docker-compose down -v
docker-compose -f docker-compose-metricstore.yml down -v