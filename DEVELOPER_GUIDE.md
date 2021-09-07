# Developer Guide

This document will walk you through on what's needed to start contributing code to OpenSearch Benchmark.

## Installation
### Prerequisites

- Pyenv : Install `pyenv` and follow the instructions in the output of `pyenv init` to setup your shell and restart it before proceeding. 
For more details please refer to the [PyEnv installation instructions](https://github.com/pyenv/pyenv#installation).
- JDK : JDK version required to build OpenSearch. Please refer to the [build setup requirements](https://github.com/opensearch-project/OpenSearch/blob/ca564fd04f5059cf9e3ce8aba442575afb3d99f1/DEVELOPER_GUIDE.md#install-prerequisites).
- Docker : Docker and additionally `docker-compose`  on Linux.
- Git : git 1.9 or latter.

### Setup

Use the following command-lin instructions to setup OpenSearch Benchmark for development :
```
git clone https://github.com/opensearch-project/OpenSearch-Benchmark.git
cd OpenSearch-Benchmark
make prereq
make install
source .venv/bin/activate
```

## Importing the project into an IDE

OpenSearch Benchmark builds using virtualenv. When importing into an IDE you will need to define an appropriate Python SDK, which is provided by virtualenv.
Refer to IDE documentation for more details on defining a Python SDK. We recommend using the Python SDK that `make prereq` creates.
This is typically created via `Virtualenv Environment` / `Existing Environment` and pointing to `.venv/bin/python3` within the OpenSearch Benchmark source directory.

In order to run tests within the IDE, ensure the `Python Integrated Tools` / `Testing` / `Default Test Runner` is set to `pytest`.

## Executing tests

Once setup is complete, you may run unit/integration tests using the following : 

```
## Run a unit test
make test

## Run integration tests
make it
```

## Submitting your changes for a pull request

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken: `python3 setup.py test`.

2. Sign the Developer Certificate of Origin

    Please make sure you have signed DCO certificate.

3. Rebase your changes

    Update your local repository with the most recent code from the main OpenSearch Benchmark repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.

4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".
    
    
## Misc

### git-secrets
Security is our top priority. Avoid checking in credentials, install awslabs/git-secrets.

```
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install
```