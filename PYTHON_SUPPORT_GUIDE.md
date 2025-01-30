# Python Support Guide

This document walks developers through how to add support for new major & minor Python versions in OpenSearch-Benchmark.

### Adding New Python Versions to OpenSearch Benchmark

1. Add a new Python major version as a tuple in this list in [setup.py](https://github.com/opensearch-project/opensearch-benchmark/blob/main/setup.py#L47). For example, if you were to want to add support for Python 3.12, you would just add it at the end of this list as a tuple.
```
# Added Python 3.12 as (3, 12) at the end of the list
supported_python_versions = [(3, 8), (3, 9), (3, 10), (3, 11), (3, 12)]
```

2. Add desired major Python version with the semantic format in [.ci/variables.json](https://github.com/opensearch-project/opensearch-benchmark/blob/main/.ci/variables.json). OpenSearch Benchmark uses semantic versioning. If you are unsure what to update the version to, use [this reference](https://semver.org/) as a guide.

3. Update version.txt accordingly. Again, OpenSearch Benchmark uses semantic versioning. If you are unsure what to update the version to, use [this reference](https://semver.org/) as a guide.

**Example of adding Python 3.10 and 3.11 versions to OpenSearch Benchmark:**
* [Commit](https://github.com/opensearch-project/opensearch-benchmark/commit/c808af899f3b168d47bb55763ede33def0e64a3b)
* [Issue](https://github.com/opensearch-project/opensearch-benchmark/issues/220)

### Testing New Python Versions After Adding to OpenSearch Benchmark

1. Setup a fresh testing environment for each of the following operating systems: MacOS, Ubuntu, and Amazon Linux 2
2. Install dependencies, git clone the repository, and install the Python version you are trying to add (whether that’s through pyenv or directly from source). If you are using pyenv, to switch to a Python version, run `pyenv local <PYTHON VERSION>`.
3. Verify the Python version your python3 alias is pointing to by running `python3 --version`. This should output the Python version you are trying to add support to OpenSearch Benchmark.
4. Run the following tests:

**Basic OpenSearch Benchmark command with distribution version and test mode**
```
opensearch-benchmark run-test --distribution-version=1.0.0 --workload=geonames --test-mode
```

**OpenSearch Benchmark command running test on target-host in test mode**
```
opensearch-benchmark run-test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'" --test-mode"
```

**OpenSearch-Benchmark command running test on target-host without test mode**
```
opensearch-benchmark run-test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

To ensure that users are using the correct python versions, install the repository with `python3 -m pip install -e .` and run `which opensearch-benchmark` to get the path. Pre-append this path to each of the three commands above and re-run them in the command line.

Keep in mind the file path outputted differs for each operating system and might point to a shell script or a Python script. Depending on which it is, make sure you can invoke it with the proper prefix such as `bash` or `python3`.

- For example: When running `which opensearch-benchmark` on an Ubuntu environment, the commad line outputs `/home/ubuntu/.pyenv/shims/opensearch-benchmark`. On closer inspection, the path points to a shell script. Thus, to invoke OpenSearch Benchmark, pre-=append the OpenSearch Benchmark command with `bash` and the path outputted earlier:
```
bash -x /home/ubuntu/.pyenv/shims/opensearch-benchmark run-test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

- Another example: When running `which opensearch-benchmark` on an Amazon Linux 2 environment, the command line outputs `~/.local/bin/opensearch-benchmark`. On closer inspection, the path points to a Python script. Thus, to invoke OpenSearch Benchmark, pre-append the OpenSearch Benchmark command with `python3` and the path outputted earlier:
```
python3 ~/.local/bin/opensearch-benchmark run-test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

### Creating a Pull Request After Adding Changes and Testing Them Out
After adding Python versions and testing them in environments, make a pull request on Github. Once that’s been merged, push a tag and get it approved by a maintainer. After this has been approved, Jenkins will publish to OpenSearch Benchmark’s Pypi account with the new version you updated it to.
```
# Create Tag
git tag <NEW MAJOR.MINOR.PATCH VERSION> main
# Push Tag
git push origin <NEW MAJOR.MINOR.PATCH VERSION>
```
