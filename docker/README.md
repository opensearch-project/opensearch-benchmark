# OpenSearch Benchmark Docker Image

This docker image allows users to spin up a docker container with all of OpenSearch-Benchmark's dependencies and get started with OSB faster.

# Run OpenSearch Benchmark Image
Ensure that you have docker installed where you plan to run the image.
1. Run the command `docker pull opensearchproject/opensearch-benchmark`. Docker will pull in the image on your host
2. To run the image to start a container, run the command `docker run opensearchproject/opensearch-benchmark`

# Build a Copy of OpenSearch Benchmark Image
1. Git clone the repository
2. Make changes locally
3. Run `docker build -t <docker image name>:<optional tag> -f ./docker/Dockerfile .`

# Publish New Version of OpenSearch Benchmark Image
After updating [version.txt](../version.txt) with the newest major, minor, and patch version and before publishing a tag to the repository, ensure that [docker.yml](../.github/workflows/docker.yml) has the tags updated. The last tag in the list should be updated to the new major, minor, and patch version found in [version.txt](../version.txt).
```
tags: opensearchstaging/opensearch-benchmark:latest,opensearchstaging/opensearch-benchmark:<MAJOR.MINOR.PATCH VERSION FROM VERSION.TXT>
```

# Best Practices for Building Dockerfiles
Follow the best practices guidelines found [here](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/) when updating OpenSearch Benchmark's Dockerfile.