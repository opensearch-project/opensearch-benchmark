# OpenSearch-Benchmark Docker Image

This docker image allows users to spin up a docker container with all of OpenSearch-Benchmark's dependencies and get started with OSB faster.

# Run OpenSearch-Benchmark Image
Ensure that you have docker installed where you plan to run the image.
1. Run the command `docker pull opensearchproject/opensearch-benchmark`. Docker will pull in the image on your host
2. To run the image to start a container, run the command `docker run opensearchproject/opensearch-benchmark`

# Build a Copy of OpenSearch-Benchmark Image
1. Make changes to the docker file
2. Run `docker build -t <docker image name>:<optional tag> -f ./docker/Dockerfile .`

Follow the best practices guidelines found [here](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/).