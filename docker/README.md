# OpenSearch Benchmark Docker Image

This Docker image allows users to spin up a Docker container preloaded with essential dependencies and get started with OpenSearch Benchmark faster.

# Running the OpenSearch Benchmark Image
**Prerequisite:** Ensure that Docker is installed. If not, refer to [this guide to download Docker Desktop](https://docs.docker.com/get-docker/) or [this guide to download Docker Engine](https://docs.docker.com/engine/install/).
1. Run the command `docker pull opensearchproject/opensearch-benchmark`. Docker will pull in the image on your host
2. To run the image and start a Docker container, run the command `docker run --entrypoint bash opensearchproject/opensearch-benchmark:latest -c "opensearch-benchmark -h"`. This will print the help screen and terminate the container. If you'd like to run it against a target OpenSearch cluster, replace `opensearch-benchmark -h` with the appropriate OSB command and arguments.
    - A simpler alternative would be to run `docker run -it opensearchproject/opensearch-benchmark /bin/sh`, which would place you into a shell to interact with the container. Now, you can invoke `opensearch-benchmark` with any desired subcommands or options. When you are finished, invoke exit command to terminate the container.


# Building a Copy of OpenSearch Benchmark Image
1. Git clone OpenSearch Benchmark Github repository
2. Make changes locally
3. Run `docker build -t <docker image name>:<optional tag> -f ./docker/Dockerfile .`

# Publishing a New Version of OpenSearch Benchmark Image
After updating [version.txt](../version.txt) with the newest major, minor, and patch version and before publishing a tag to the repository, ensure that [docker.yml](../.github/workflows/docker.yml) has the tags updated. The last tag in the list should be updated to the new major, minor, and patch version found in [version.txt](../version.txt).
```
tags: opensearchstaging/opensearch-benchmark:latest,opensearchstaging/opensearch-benchmark:<MAJOR.MINOR.PATCH VERSION FROM VERSION.TXT>
```

# Applying Best Practices for Building Dockerfiles
Follow the best practices guidelines found [here](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/) when updating OpenSearch Benchmark's Dockerfile.