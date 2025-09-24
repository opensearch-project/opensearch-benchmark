# Python Support Guide

This document walks developers through how to add support for new major & minor Python versions in OpenSearch-Benchmark.

### Update Python versions supported in OpenSearch Benchmark

* `./ci/variables.json`: Add or remove python variables and update `MIN_PY_VER` if necessary.
    * For example, to add Python `3.12`, ensure  `PY312` exists and set it to the latest patched version of `3.12`, such as `3.12.11`.
* `.github/workflows/integ-test.yml`: Add or remove Python versions to `python-version`
* `setup.py`: Add to or remove from `supported_python_versions` tuples.
* `tox.ini`: Add to or remove from `envlist =` section.
* `osbenchamrk/__init__.py`: Update the following line's `<MINIMUM PYTHON VERSION SUPPORTED>` if the minimum supported Python version was updated.
```
raise RuntimeError("OSB requires at least <MINIMUM PYTHON VERSION SUPPORTED> but you are using:\n\nPython %s" % str(sys.version))`
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
