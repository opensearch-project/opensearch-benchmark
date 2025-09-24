# Python Support Guide

This document walks developers through how to add support for new major & minor Python versions in OpenSearch-Benchmark.

### Update Python versions supported in OpenSearch Benchmark

Make changes to the following files and open a PR titled "Update Python versions supported to <list of python versions this PR plans to support>.

* `.ci/variables.json`: Update Python variables and `MIN_PY_VER` as needed.
    * For example: If OSB needs to add support for Python `3.12`, ensure there is a `PY312` variable and make it set to the latest patch release of Python `3.12`, such as `3.12.11`.
* `.github/workflows/integ-tests.yml`: Update supported Python versions in the `python-versions` section
* `setup.py`: Update supported Python versions in `supported_python_versions`.
* `tox.ini`: Update supported Python versions in `env_list`
* `Makefile`: If updating the minimum supported Python version, ensure that the minimum Python version environment variable, the `pyinst<MINIMUM_SUPPORTED_PYTHON_VERSION>` section, and `check-pip` section have been updated.
    * For example: If changing the minimum supported Python version to Python `3.10`, ensure the following lines have been updated to use 3.10
```
VERSION310 = $(shell jq -r '.python_versions | .[]' .ci/variables.json | sed '$$d' | grep 3\.10)

pyinst310:
    pyenv install --skip-existing $(VERSION310)
	pyenv local $(VERSION310)

check-pip:
    @if ! $(PIP) > /dev/null 2>&1 || ! $(PIP) install pip > /dev/null 2>&1; then make pyinst310; fi
```
* `osbenchmark/__init__.py`: If updating the minimum supported Python version, ensure the <MINIMUM_SUPPORTED_PYTHON_VERSION> has been updated in the following error statement:
```
raise RuntimeError("OSB requires at least Python <MINIMUM_SUPPORTED_PYTHON_VERSION> but you are using:\n\nPython %s" % str(sys.version))
```

For an example, please see the reference the following:
* [Update Python versions supported to 3.10 - 3.13](https://github.com/opensearch-project/opensearch-benchmark/pull/961/files)

### Testing New Python Versions After Adding to OpenSearch Benchmark

1. Setup a fresh testing environment for each of the following operating systems: MacOS, Ubuntu, and Amazon Linux 2
2. Install dependencies, git clone the repository, and install the Python version you are trying to add (whether that’s through pyenv or directly from source). If you are using pyenv, to switch to a Python version, run `pyenv local <PYTHON VERSION>`.
3. Verify the Python version your python3 alias is pointing to by running `python3 --version`. This should output the Python version you are trying to add support to OpenSearch Benchmark.
4. Run the following tests:

**Basic OpenSearch Benchmark command with distribution version and test mode**
```
opensearch-benchmark run --distribution-version=1.0.0 --workload=geonames --test-mode
```

**OpenSearch Benchmark command running test on target-host in test mode**
```
opensearch-benchmark run --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'" --test-mode"
```

**OpenSearch-Benchmark command running test on target-host without test mode**
```
opensearch-benchmark run --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

To ensure that users are using the correct python versions, install the repository with `python3 -m pip install -e .` and run `which opensearch-benchmark` to get the path. Pre-append this path to each of the three commands above and re-run them in the command line.

Keep in mind the file path outputted differs for each operating system and might point to a shell script or a Python script. Depending on which it is, make sure you can invoke it with the proper prefix such as `bash` or `python3`.

- For example: When running `which opensearch-benchmark` on an Ubuntu environment, the commad line outputs `/home/ubuntu/.pyenv/shims/opensearch-benchmark`. On closer inspection, the path points to a shell script. Thus, to invoke OpenSearch Benchmark, pre-=append the OpenSearch Benchmark command with `bash` and the path outputted earlier:
```
bash -x /home/ubuntu/.pyenv/shims/opensearch-benchmark run --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

- Another example: When running `which opensearch-benchmark` on an Amazon Linux 2 environment, the command line outputs `~/.local/bin/opensearch-benchmark`. On closer inspection, the path points to a Python script. Thus, to invoke OpenSearch Benchmark, pre-append the OpenSearch Benchmark command with `python3` and the path outputted earlier:
```
python3 ~/.local/bin/opensearch-benchmark run --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

### Creating a Pull Request After Adding Changes and Testing Them Out
After adding Python versions and testing them in environments, make a pull request on Github. Once that’s been merged, push a tag and get it approved by a maintainer. After this has been approved, Jenkins will publish to OpenSearch Benchmark’s Pypi account with the new version you updated it to.
```
# Create Tag
git tag <NEW MAJOR.MINOR.PATCH VERSION> main
# Push Tag
git push origin <NEW MAJOR.MINOR.PATCH VERSION>
```
