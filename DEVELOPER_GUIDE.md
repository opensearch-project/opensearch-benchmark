# Developer Guide

This document will walk you through on what's needed to start contributing code to OpenSearch Benchmark.

### Table of Contents
- [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
- [Importing the project into an IDE](#importing-the-project-into-an-ide)
- [Setting Up a Local OpenSearch Cluster For OSB Development (Optional)](#setting-up-a-local-opensearch-cluster-for-osb-development-optional)
- [Executing tests](#executing-tests)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
- [Submitting your changes for a pull request](#submitting-your-changes-for-a-pull-request)
- [Developing breaking changes](#developing-breaking-changes)
- [Miscellaneous](#miscellaneous)
    - [git-secrets](#git-secrets)
    - [Adding new major and minor Python versions to OpenSearch Benchmark](#adding-new-major-and-minor-python-versions-to-opensearch-benchmark)
    - [Debugging OpenSearch Benchmark in Developer Mode](#debugging-opensearch-benchmark-in-developer-mode)
    - [Debugging unittests in Visual Studio Code](#debugging-unittests-in-visual-studio-code)

## Installation

### Prerequisites

  - **pyenv**: Install `pyenv` and follow the instructions in the output of `pyenv init` to set up your shell and restart it before proceeding.
    For more details please refer to the [pyenv installation instructions](https://github.com/pyenv/pyenv#installation).

    `pyenv` requires that the C compiler and development libraries be installed, so that the specified Python versions can be build from source.  The installation instructions vary from platform to platform.

    For Debian-based systems, install the following modules to continue with the next steps: 
    ```
    sudo apt-get install -y make build-essential libssl-dev zlib1g-dev libbz2-dev \
    libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev \
    xz-utils tk-dev libffi-dev liblzma-dev git
    ```

    For Amazon Linux 2023, run the following command:
    ```
    sudo yum -y install gcc openssl-devel bzip2-devel libffi-devel ncurses-devel sqlite-devel readline-devel zlib-devel xz-devel
    ```

    On the Mac platform, XCode needs to be installed as well as some additional required libraries:
    ```
    xcode-select --install
    brew install pyenv jq zlib xz
    ```

  - **JDK**: Although OSB is a Python application, it optionally builds and provisions OpenSearch clusters.  JDK version 17 is used to build the current version of OpenSearch.  Please refer to the [build setup requirements](https://github.com/opensearch-project/OpenSearch/blob/ca564fd04f5059cf9e3ce8aba442575afb3d99f1/DEVELOPER_GUIDE.md#install-prerequisites).
    Note that the `javadoc` executable should be available in the JDK installation.  An earlier version of the JDK can be used, but not all the integration tests will pass.

    ```
    export JAVA_HOME=/path/to/JDK17

    ```

  - **Docker**: Install Docker and `docker-compose`. Start the Docker server. The user running the integration tests should have the permissions required to run docker commands. Test by running `docker ps`.

  - **Git** : supports versions 1.9+

### Setup

To develop OSB properly, it is recommended that you fork the official OpenSearch Benchmark repository. 

For those working on WSL2, it is recommended to clone the repository and set up the working environment within the Linux subsystem. Refer to the guide for setting up WSL2 on [Visual Studio Code](https://code.visualstudio.com/docs/remote/wsl) or [PyCharm](https://www.jetbrains.com/help/pycharm/using-wsl-as-a-remote-interpreter.html#create-wsl-interpreter). 

After you git cloned the forked copy of OpenSearch Benchmark, use the following command-line instructions to set up OpenSearch Benchmark for development:
```
cd opensearch-benchmark
make develop
```

Depending on the platform and shell you have, use the following command to activate the virtual environment:

| Platform | Shell | Command used to activate the virtual environment |
| --------------- | --------- | ------------------------------------- |
| Posix| bash/zsh | source .venv/bin/activate |
| | fish | source .venv/bin/activate.fish |
| | csh/tcsh | source .venv/bin/activate.csh |
|  | PowerShell Core | .venv/bin/Activate.ps1 |
| Windows | cmd.exe| C:\> <venv>\Scripts\activate.bat |
| | PowerShell | PS C:\> <venv>\Scripts\Activate.ps1 |

For more information regarding activating virtual environments, please see https://docs.python.org/3/library/venv.html.

## Importing the project into an IDE

OpenSearch Benchmark builds using virtualenv. When importing into an IDE, such as PyCharm IDE, you will need to define an appropriate Python SDK, which is provided by virtualenv.
Refer to IDE documentation for more details on defining a Python SDK. We recommend using the Python SDK that `make prereq` creates.
This is typically created in PyCharm IDE by visiting the `Python Interpreter`, selecting either `Virtualenv Environment` or `Existing Environment`, and pointing interpreter to `.venv/bin/python3` within the OpenSearch Benchmark source directory.
`
In order to run tests within the PyCharm IDE, ensure the `Python Integrated Tools` / `Testing` / `Default Test Runner` is set to `pytest`.

## Setting Up a Local OpenSearch Cluster For OSB Development (Optional)

### OpenSearch Installation  

Download the latest release of OpenSearch from https://opensearch.org/downloads.html. If you are using WSL, make sure to download it into your `/home/<user>` directory instead of `/mnt/c`. 
```
wget https://artifacts.opensearch.org/releases/bundle/opensearch/<x.x.x>/opensearch-<x.x.x>-linux-x64.tar.gz
tar -xf opensearch-x.x.x-linux-x64.tar.gz
cd opensearch-x.x.x
```
NOTE: Have Docker running in the background for the next steps. Refer to the installation instructions [here](https://docs.docker.com/compose/install/).

### OpenSearch Cluster setup

Add the following settings to the `opensearch.yml` file under the config directory 
```
vim config/opensearch.yml
```
```
#
discovery.type: single-node 
plugins.security.disabled: true 
#
```
Run the opensearch-tar-install.sh script to install and setup a cluster for our use. 
```
bash opensearch-tar-install.sh
```
Check the output of `curl.exe "http://localhost:9200/_cluster/health?pretty"`. Output should be similar to this:
```
{
  "cluster_name" : "<name>",
  "status" : "green",
  "timed_out" : false,
  "number_of_nodes" : 1,
  "number_of_data_nodes" : 1,
  "discovered_master" : true,
  "discovered_cluster_manager" : true,
  "active_primary_shards" : 3,
  "active_shards" : 3,
  "relocating_shards" : 0,
  "initializing_shards" : 0,
  "unassigned_shards" : 0,
  "delayed_unassigned_shards" : 0,
  "number_of_pending_tasks" : 0,
  "number_of_in_flight_fetch" : 0,
  "task_max_waiting_in_queue_millis" : 0,
  "active_shards_percent_as_number" : 100.0
}
```
Now, you have a local cluster running! You can connect to this and run the workload for the next step. 

### Running Workloads on a locally installed Cluster

Here's a sample executation of the geonames benchmark which can be found from the [workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads) repo. 
```
opensearch-benchmark execute-test --pipeline=benchmark-only --workload=geonames --target-host=127.0.0.1:9200 --test-mode --workload-params '{"number_of_shards":"1","number_of_replicas":"0"}'
```

And we're done! You should be seeing the performance metrics soon enough!

### Debugging 

**If you are not seeing any results, it should be an indicator that there is an issue with your cluster setup or the way the manager is accessing it**. Use the command below to view the logs. 
```
tail -f ~/.benchmark/logs/benchmark.log
```

## Executing tests

Once setup is complete, you may run the unit and integration tests.

### Unit Tests
Invoke unit tests by running the following command within the root directory of the repository:

```
make test
```

### Integration Tests

Integration tests are expected to run for approximately **20-30 mins** and can be run on the following operating systems:
  * RedHat
  * CentOS
  * Ubuntu (and WSL)
  * Amazon Linux 2
  * MacOS

Integration tests run against the standard [OpenSearch Benchmark workloads](https://github.com/opensearch-project/opensearch-benchmark-workloads).  Sometimes, it may be necessary to run integration tests against a modified forked copy of these workloads.  In that case, please follow the instructions [here](https://github.com/opensearch-project/opensearch-benchmark-workloads/blob/main/README.md#testing-the-workload).

Invoke integration tests by running the following command within the root directory of the repository:

```
make it
```


## Submitting your changes for a pull request

Once your changes and tests are ready to submit for review:

1. Test your changes

    Run the test suite to make sure that nothing is broken: `make it`.

2. Sign the Developer Certificate of Origin

    Please make sure you have signed the DCO certificate. Include the `--signoff` argument as part of your `git commit`

3. Rebase your changes

    Update your local repository with the most recent code from the main OpenSearch Benchmark repository, and rebase your branch on top of the latest master branch. We prefer your initial changes to be squashed into a single commit. Later, if we ask you to make changes, add them as separate commits.  This makes them easier to review.  As a final step before merging we will either ask you to squash all commits yourself or we'll do it for you.

4. Submit a pull request

    Push your local changes to your forked copy of the repository and [submit a pull request](https://help.github.com/articles/using-pull-requests). In the pull request, choose a title which sums up the changes that you have made, and in the body provide more details about what your changes do. Also mention the number of the issue where discussion has taken place, eg "Closes #123".

## Developing Breaking Changes
Breaking changes should not be directly added to the `main` branch. These should be developed in their own feature branch. Prior to a new release this feature branch should be rebased onto the latest changes from `main`. `main` can then `pull` or `cherry-pick` the breaking changes from the feature branch.

## Miscellaneous

### git-secrets
Security is our top priority. Avoid checking in credentials, install awslabs/git-secrets.

```
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install
```
### Adding New Major and Minor Python Versions to OpenSearch-Benchmark
To streamline the process, please refer to [this guide](https://github.com/opensearch-project/opensearch-benchmark/blob/main/PYTHON_SUPPORT_GUIDE.md)

### Debugging OpenSearch Benchmark in Developer Mode
Many users find that the simplest way to debug OpenSearch Benchmark is by using developer mode. Users can activate developer mode by running `python3 -m pip install -e .` within the cloned OpenSearch Benchmark repository. Any changes made and saved will be reflected when OpenSearch Benchmark is run. Users can add loggers or print statements and see the changes reflected in subsequent runs. 

### Debugging Unittests in Visual Studio Code
To run and debug unittests in Visual Studio Code, add the following configuration to the Python Debugger `launch.json` file. See [the official Visual Studio Code documentation](https://code.visualstudio.com/docs/editor/debugging) for more information on setting up and accessing `launch.json` file.
```
        {
            "name": "Python: Module",
            "type": "python",
            "request": "launch",
            "module": "pytest",
            "args": ["-k ${file}"]
        }
```
With this, users can easily run and debug unittests within Visual Studio Code without invoking pytest manually on the command line.
