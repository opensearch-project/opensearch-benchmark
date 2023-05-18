# OpenSearch Benchmark Docker Image

This Docker image allows users to spin up a Docker container preloaded with essential dependencies and get started with OpenSearch Benchmark faster.

# Running the OpenSearch Benchmark Image
**Prerequisite:** Ensure that Docker is installed. If not, refer to [this guide to download Docker Desktop](https://docs.docker.com/get-docker/) or [this guide to download Docker Engine](https://docs.docker.com/engine/install/).

To run the image in a Docker container, invoke the following command:
```
docker run opensearchproject/opensearch-benchmark [OSB ARGS]
```

For instance, using `-h` for the arguments will print the OSB help information. Once the OSB process completes, the Docker container is automatically terminated.

To run in interactive mode, run `docker run --entrypoint bash -it opensearchproject/opensearch-benchmark -c /bin/bash`. This will place you into a shell to interact with the container where you can invoke opensearch-benchmark with any desired subcommands or options. When you are finished, exit from the shell to terminate the container.


The 0.4.1 release of OSB includes a Beta feature that permits increasing the size of the data corpus associated with the _http_logs_ workload.  For more details, run the command below:
```
docker run -it --entrypoint expand-data-corpus.py opensearchstaging/opensearch-benchmark:0.4.1 -h
```


# Building a Copy of OpenSearch Benchmark Image
1. Git clone OpenSearch Benchmark Github repository
2. Make changes locally
3. Run `docker build --build-arg VERSION=<PYPI VERSION> -t <docker image name>:<optional tag> -f ./docker/Dockerfile .`

Note: `Dockerfile` uses the argument `VERSION` to install a specific version of OSB that exists on PyPi.

### Difference between Dockerfile and Dockerfile-development
The build command in step 3 uses `Dockerfile`, which installs OSB from Pypi. However, if users want an image with OSB installed from the source code, replace `Dockerfile` in step 3's command with `Dockerfile-development`. When using `Dockerfile-development`, users can omit the `--build-arg VERSION=<PYPI VERSION>` since this argument is not used when installing OSB from source code.


# Publishing a New Version of OpenSearch Benchmark Image
After updating [version.txt](../version.txt) with the newest major, minor, and patch version and before publishing a tag to the repository, ensure that [docker.yml](../.github/workflows/docker.yml) has the tags updated. The last tag in the list should be updated to the new major, minor, and patch version found in [version.txt](../version.txt).
```
tags: opensearchstaging/opensearch-benchmark:latest,opensearchstaging/opensearch-benchmark:<MAJOR.MINOR.PATCH VERSION FROM VERSION.TXT>
```

# Applying Best Practices for Building Dockerfiles
Follow the best practices guidelines found [here](https://docs.docker.com/develop/develop-images/dockerfile_best-practices/) when updating OpenSearch Benchmark's Dockerfile.