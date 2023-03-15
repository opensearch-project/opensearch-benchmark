# Python Support Guide

This document walks developers on how to add support for new major & minor Python versions in OpenSearch-Benchmark.

### Add New Python Versions to OpenSearch-Benchmark

1. Add new python major version as tuple in list in [setup.py](https://github.com/opensearch-project/opensearch-benchmark/blob/main/setup.py#L47). For example, if you were to want to add support for python 3.12 to this, you would just add it at the end of this list as a tuple.
```
# Added Python 3.12 as (3, 12) at the end of the list
supported_python_versions = [(3, 8), (3, 9), (3, 10), (3, 11), (3, 12)]
```

2. Add desired major python version with the semantic format in [.ci/variables.json](https://github.com/opensearch-project/opensearch-benchmark/blob/main/.ci/variables.json). Opensearch-benchmark uses semantic versioning. If you are unsure what to update the version to, use this reference as a guide https://semver.org/.

3. Update version.txt accordingly. Again, Opensearch-benchmark uses semantic versioning. If you are unsure what to update the version to, use this reference as a guide https://semver.org/.

**Example of adding Python 3.10 and 3.11 versions to opensearch-benchmark:**
* [Commit](https://github.com/opensearch-project/opensearch-benchmark/commit/c808af899f3b168d47bb55763ede33def0e64a3b)
* [Issue](https://github.com/opensearch-project/opensearch-benchmark/issues/220)

### Test New Python Versions After Adding to OpenSearch-Benchmark

1. Setup a fresh testing environment for each of the following operating systems: MacOS, Ubuntu, and Amazon Linux 2
    - Refer to the instructions in this document to setup environments OpenSearch-Benchmark PyPi Improvements
2. Install dependencies, git clone the repository, and install the python version you are trying to add (whether that’s through pyenv or directly from source). If you are using pyenv, to switch to a python version, run `pyenv local <PYTHON VERSION>`.
3. Verify python version your python3 alias is pointing to by running `python3 --version`. This should output the python version you are trying to add support to opensearch-benchmark.
4. Run the following tests:

**Basic OpenSearch-Benchmark command with distribution version and test mode**
```
opensearch-benchmark execute_test --distribution-version=1.0.0 --workload=geonames --test-mode
```

**OpenSearch-Benchmark command executing test on target-host in test mode**
```
opensearch-benchmark execute_test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'" --test-mode"
```

**OpenSearch-Benchmark command executing test on target-host without test mode**
```
opensearch-benchmark execute_test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

For commands above, run them like shown above and then run them with the path that’s outputted with which opensearch-benchmark after installing the repository with `python3 -m pip install -e .`. Keep in mind the file path outputted might point to a shell script or a python script. Depending on which it is, make sure you can invoke it with the proper prefix such as bash or python3.

For example: When I run which opensearch-benchmark on my Ubuntu environment, I get /home/ubuntu/.pyenv/shims/opensearch-benchmark. The path points to a shell script. Thus, I run the following to invoke a command:
```
bash -x /home/ubuntu/.pyenv/shims/opensearch-benchmark execute_test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

- Another example: When I run which opensearch-benchmark on my Amazon Linux 2 environment, I get ~/.local/bin/opensearch-benchmark. This path points to a python script. Thus, I run the following to invoke a command:
```
python3 ~/.local/bin/opensearch-benchmark execute_test --workload=geonames --pipeline=benchmark-only --target-host="<OPENSEARCH CLUSTER ENDPOINT>" --client-options="basic_auth_user:'<USERNAME>',basic_auth_password:'<PASSWORD>'"
```

### Creating a Pull Request After Adding Changes and Testing Them Out
After adding python versions and testing them in environments, make a pull request on Github. Once that’s been merged, push a tag and get it approved by a maintainer. After this has been approved, Jenkins will publish to opensearch-benchmark’s Pypi account with the new version you updated it to.
```
# Create Tag
git tag <NEW MAJOR.MINOR.PATCH VERSION> main
# Push Tag
git push origin <NEW MAJOR.MINOR.PATCH VERSION>
```
