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

Use the following command-line instructions to setup OpenSearch Benchmark for development:
```
git clone https://github.com/opensearch-project/OpenSearch-Benchmark.git
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

As an extra step, please consider configuring your JAVA_HOMES as mentioned in the `Important information related to integration tests`.

## Importing the project into an IDE

OpenSearch Benchmark builds using virtualenv. When importing into an IDE, such as PyCharm IDE, you will need to define an appropriate Python SDK, which is provided by virtualenv.
Refer to IDE documentation for more details on defining a Python SDK. We recommend using the Python SDK that `make prereq` creates.
This is typically created in PyCharm IDE by visiting the `Python Interpreter`, selecting either `Virtualenv Environment` or `Existing Environment`, and pointing interpreter to `.venv/bin/python3` within the OpenSearch Benchmark source directory.
`
In order to run tests within the PyCharm IDE, ensure the `Python Integrated Tools` / `Testing` / `Default Test Runner` is set to `pytest`.

## Executing tests

Once setup is complete, you may run unit/integration tests using the following :

```
## Run a unit test
make test

## Run integration tests
make it
```

We recommend users to look at the following two sections [Common issues in unittests](#common-issues-in-unittests) and [Important information related to integration tests](#important-information-related-to-integration-tests).

### Common issues in Unittests
#### OSB_DATASTORE_PASSWORD environment variable conflicts with unittests
OSB has the ability to store metrics into an OpenSearch datastore. To enable this, users can write the datastore password to the `benchmark.ini` config file or export it to the environment variable `OSB_DATASTORE_PASSWORD`. However, when the latter is done, users might encounter an assertion error when running unittests. This is because one unittest uses values assigned to `OSB_DATASTORE_PASSWORD`.

The following is an example of the assertion error that comes up when users run unittests while the environment variable `OSB_DATASTORE_PASSWORD` is set.
```
>           raise AssertionError(_error_message()) from cause
E           AssertionError: expected call not found.
E           Expected: OsClientFactory(hosts=[{'host': '27.158.71.181', 'port': 27959}], client_options={'use_ssl': True, 'timeout': 120, 'basic_auth_user': 'WuxfoBBk', 'basic_auth_password': 'JXkTu9JhzFq$', 'verify_certs': False})
E           Actual: OsClientFactory(hosts=[{'host': '27.158.71.181', 'port': 27959}], client_options={'use_ssl': True, 'verify_certs': False, 'timeout': 120, 'basic_auth_user': 'WuxfoBBk', 'basic_auth_password': '<Value from OSB_DATASTORE_PASSWORD Environment Variable>'})

../../../../.pyenv/versions/3.8.12/lib/python3.8/unittest/mock.py:913: AssertionError

...

=========================== short test summary info ============================
FAILED tests/metrics_test.py::OsClientTests::test_config_opts_parsing_with_config - AssertionError: expected call not found.
============ 1 failed, 1163 passed, 5 skipped, 3 warnings in 15.41s ============
```
To avoid this issue when running unittests, unset your datastore password by running `unset OSB_DATASTORE_PASSWORD`.


### Important information related to integration tests

To have all the tests run successfully JDK 17 is required, since one of the tests builds the latest version of OpenSearch from source, and the OpenSearch project uses JDK 17 for this purpose.
```
export JAVA_HOME=/path/to/JDK17

```

Note that the `javadoc` executable should be available in the JDK installation.

You could also use one of the following versions instead: 16, 15, 14, 13, 12, 11 or 8.  Most of the complement of tests included will work with these.

If you have multiple JDKs installed, export them in the following format `JAVA(jdk_version)_HOME`. Here is an example of how one would export JDK 8, 11, 15, 16:
```
export JAVA8_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_231.jdk/Contents/Home/
export JAVA11_HOME=/Library/Java/JavaVirtualMachines/jdk-11.0.8.jdk/Contents/Home
export JAVA15_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-15.jdk/Contents/Home/
export JAVA16_HOME=/Library/Java/JavaVirtualMachines/amazon-corretto-16.jdk/Contents/Home/
```

OpenSearch currently releases artifacts for the following operating systems:
- Linux
- Docker
- FreeBSD

If the operating system is not listed above then the artifacts used will default to Linux.

For MacOS users running OpenSearch, please set JAVA_HOME to one of the local JDKs you exported as the JDK bundled with OpenSearch is not compatible with MacOS and prevent the metrics store from coming up correctly during integration tests.

### Troubleshooting
```
root WARNING Unable to get address info for address xxxxyyyy (AddressFamily.AF_INET, SocketKind.SOCK_DGRAM, 17, 0): <class 'socket.gaierror'> [Errno 8] nodename nor servname provided, or not known
```
- VPN may be interfering. Try turning off the VPN. If you are connected to a VPN and face Docker-related issues when running the integration tests disconnecting from the VPN may fix this.

```
Cannot download OpenSearch-1.0.1
```
- JAVA_HOME may not be correctly set.


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

## Misc

### git-secrets
Security is our top priority. Avoid checking in credentials, install awslabs/git-secrets.

```
git clone https://github.com/awslabs/git-secrets.git
cd git-secrets
make install
```
### Adding New Major and Minor Python Versions to OpenSearch-Benchmark
To streamline the process, please refer to [this guide](./PYTHON_SUPPORT_GUIDE.md)

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
