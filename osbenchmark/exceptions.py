# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
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


class BenchmarkError(Exception):
    """
    Base class for all Benchmark exceptions
    """

    def __init__(self, message, cause=None):
        super().__init__(message, cause)
        self.message = message
        self.cause = cause

    def __repr__(self):
        return self.message

    def __str__(self):
        return self.message


class LaunchError(BenchmarkError):
    """
    Thrown whenever there was a problem launching the benchmark candidate
    """


class SystemSetupError(BenchmarkError):
    """
    Thrown when a user did something wrong, e.g. the metrics store is not started or required software is not installed
    """


class BenchmarkAssertionError(BenchmarkError):
    """
    Thrown when a (precondition) check has been violated.
    """


class BenchmarkTaskAssertionError(BenchmarkAssertionError):
    """
    Thrown when an assertion on a task has been violated.
    """


class ConfigError(BenchmarkError):
    pass


class DataError(BenchmarkError):
    """
    Thrown when something is wrong with the benchmark data
    """


class SupplyError(BenchmarkError):
    pass


class BuildError(BenchmarkError):
    pass


class InvalidSyntax(BenchmarkError):
    pass


class InvalidName(BenchmarkError):
    pass


class WorkloadConfigError(BenchmarkError):
    """
    Thrown when something is wrong with the workload config e.g. user supplied a workload-param
    that can't be set
    """


class NotFound(BenchmarkError):
    pass
