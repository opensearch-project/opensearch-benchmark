# Developer Guide

This document will walk you through on what's needed to start contributing code to OpenSearch Benchmark.

### Table of Contents
- [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Setup](#setup)
- [Importing the project into an IDE](#importing-the-project-into-an-ide)
- [Executing tests](#executing-tests)
    - [Unit tests](#unit-tests)
    - [Integration tests](#integration-tests)
- [Submitting your changes for a pull request](#submitting-your-changes-for-a-pull-request)
- [Developing breaking changes](#developing-breaking-changes)
- [Miscellaneous](#miscellaneous)
    - [git-secrets](#git-secrets)
    - [Adding new major and minor Python versions to OpenSearch Benchmark](#adding-new-major-and-minor-python-versions-to-opensearch-benchmark)
    - [Debugging unittests in Visual Studio Code](#debugging-unittests-in-visual-studio-code)

## Installation

### Prerequisites

  - **Pyenv** : Install `pyenv` and follow the instructions in the output of `pyenv init` to set up your shell and restart it before proceeding.
    For more details please refer to the [PyEnv installation instructions](https://github.com/pyenv/pyenv#installation).

  - **JDK**: Although OSB is a Python application, it optionally builds and provisions OpenSearch clusters.  JDK version 17 is used to build the current version of OpenSearch.  Please refer to the [build setup requirements](https://github.com/opensearch-project/OpenSearch/blob/ca564fd04f5059cf9e3ce8aba442575afb3d99f1/DEVELOPER_GUIDE.md#install-prerequisites).
    Note that the `javadoc` executable should be available in the JDK installation.  An earlier version of the JDK can be used, but not all the integration tests will pass.

    ```
    export JAVA_HOME=/path/to/JDK17

    ```

  - **Docker**: Install Docker and `docker-compose`. Start the Docker server. The user running the integration tests should have the permissions required to run docker commands. Test by running `docker ps`.

  - **Git** : supports versions 1.9+

### Setup

To develop OSB properly, it is recommended that you fork the official OpenSearch Benchmark repository.

After you git cloned the forked copy of OpenSearch Benchmark, use the following command-line instructions to set up OpenSearch Benchmark for development:
```
cd OpenSearch-Benchmark
make prereq
make install
```

NOTE: `make prereq` produces the following message.
```
IMPORTANT: please add `eval "$(pyenv init -)"` to your bash profile and restart your terminal before proceeding any further.
```
This line is commonly thought of as an error message but rather it's just a warning. Unless you haven't already added `eval "$(pyenv init -)"` to your bash profile and restarted your terminal, then feel free to proceed forward. This eval statement is necessary in the startup configuration as it allows Pyenv to manage python versions by adding python shims to your path. If you experience any issues, please see https://github.com/pyenv/pyenv.

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

## Executing tests

Once setup is complete, you may run the unit and integration tests.

### Unit Tests
Invoke unit tests by running the following command within the root directory of the repository:

```
make test
```

### Integration Tests

Integration tests can be run on the following operating systems:
  * RedHat
  * CentOS
  * Ubuntu
  * Amazon Linux 2
  * MacOS

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