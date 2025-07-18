# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

SHELL = /bin/bash
PYTHON = python3
PIP = pip3
VERSIONS = $(shell jq -r '.python_versions | .[]' .ci/variables.json | sed '$$d')
VERSION38 = $(shell jq -r '.python_versions | .[]' .ci/variables.json | sed '$$d' | grep 3\.8)
PYENV_ERROR = "\033[0;31mIMPORTANT\033[0m: Please install pyenv and run \033[0;31meval \"\$$(pyenv init -)\"\033[0m.\n"

all: develop

pyinst:
	@which pyenv > /dev/null 2>&1 || { printf $(PYENV_ERROR); exit 1; }
	@for i in $(VERSIONS); do pyenv install --skip-existing $$i; done
	pyenv local $(VERSIONS)

pyinst38:
	@which pyenv > /dev/null 2>&1 || { printf $(PYENV_ERROR); exit 1; }
	pyenv install --skip-existing $(VERSION38)
	pyenv local $(VERSION38)

check-pip:
	# Install pyenv if the Python environment is externally managed.
	@if ! $(PIP) > /dev/null 2>&1 || ! $(PIP) install pip > /dev/null 2>&1; then make pyinst38; fi

check-java:
	@if ! test "$(JAVA21_HOME)" || ! java --version > /dev/null 2>&1 || ! javadoc --help > /dev/null 2>&1; then \
	    echo "Java installation issues for running integration tests" >&2; \
	    exit 1; \
	fi
	@if test `java --version | sed -n 's/[^0-9]*\([0-9]*\).*./\1/p;q'` != 17; then \
	    echo "NOTE: Java version 17 required to have all integration tests pass" >&2; \
	fi

install-deps: check-pip
	$(PIP) install --upgrade pip setuptools wheel

# pylint does not work with Python versions >3.8:
#   Value 'Optional' is unsubscriptable (unsubscriptable-object)
develop: pyinst38 install-deps
	PIP_ONLY_BINARY=h5py $(PIP) install -e .[develop]

build: install-deps
	$(PIP) install --upgrade build
	$(PYTHON) -m build

# Builds a wheel from source, then installs it.
install: build
	PIP_ONLY_BINARY=h5py $(PIP) install dist/opensearch_benchmark-*.whl
	rm -rf dist

clean:
	rm -rf .benchmarks .eggs .tox .benchmark_it .cache build dist *.egg-info logs junit-py*.xml *.whl NOTICE.txt

# Avoid conflicts between .pyc/pycache related files created by local Python interpreters and other interpreters in Docker
python-caches-clean:
	-@find . -name "__pycache__" -prune -exec rm -rf -- \{\} \;
	-@find . -name ".pyc" -prune -exec rm -rf -- \{\} \;

# Note: pip will not update project dependencies (specified either in the install_requires or the extras
# section of the setup.py) if any version is already installed; therefore we recommend
# recreating your environments whenever your project dependencies change.
tox-env-clean:
	rm -rf .tox

lint:
	@find osbenchmark benchmarks scripts tests it -name "*.py" -exec pylint -j0 -rn --load-plugins pylint_quotes --rcfile=$(CURDIR)/.pylintrc \{\} +

test: develop
	pytest tests/

it: pyinst check-java python-caches-clean tox-env-clean
	@which tox || $(PIP) install tox
	tox

it38 it39 it310 it311: pyinst check-java python-caches-clean tox-env-clean
	@which tox || $(PIP) install tox
	tox -e $(@:it%=py%)

benchmark:
	pytest benchmarks/

coverage:
	coverage run setup.py test
	coverage html

release-checks:
	./release-checks.sh $(release_version) $(next_version)

# usage: e.g. make release release_version=0.9.2 next_version=0.9.3
release: release-checks clean it
	./release.sh $(release_version) $(next_version)

.PHONY: install clean python-caches-clean tox-env-clean test it it38 benchmark coverage release release-checks pyinst
