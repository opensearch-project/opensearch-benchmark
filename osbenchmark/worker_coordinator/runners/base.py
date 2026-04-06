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

import logging

from osbenchmark import exceptions
from osbenchmark.context import RequestContextHolder

class Delegator:
    """
    Mixin to unify delegate handling
    """
    def __init__(self, delegate, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delegate = delegate

class Runner:
    """
    Base class for all benchmark operations.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        return self

    async def __call__(self, opensearch, params):
        """
        Runs the actual method that should be benchmarked.

        :param args: All arguments that are needed to call this method.
        :return: A pair of (int, String). The first component indicates the "weight" of this call. it is typically 1 but for bulk operations
                 it should be the actual bulk size. The second component is the "unit" of weight which should be "ops" (short for
                 "operations") by default. If applicable, the unit should always be in plural form. It is used in metrics records
                 for throughput and results. A value will then be shown as e.g. "111 ops/s".
        """
        raise NotImplementedError("abstract operation")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def _default_kw_params(self, params):
        # map of API kwargs to OSB config parameters
        kw_dict = {
            "body": "body",
            "headers": "headers",
            "index": "index",
            "opaque_id": "opaque-id",
            "params": "request-params",
            "request_timeout": "request-timeout",
        }
        full_result =  {k: params.get(v) for (k, v) in kw_dict.items()}
        # filter Nones
        return dict(filter(lambda kv: kv[1] is not None, full_result.items()))

    def _transport_request_params(self, params):
        request_params = params.get("request-params", {})
        request_timeout = params.get("request-timeout")
        if request_timeout is not None:
            request_params["request_timeout"] = request_timeout
        headers = params.get("headers") or {}
        opaque_id = params.get("opaque-id")
        if opaque_id is not None:
            headers.update({"x-opaque-id": opaque_id})
        return request_params, headers

request_context_holder = RequestContextHolder()

class WithCompletion(Runner, Delegator):
    def __init__(self, delegate, progressable):
        super().__init__(delegate=delegate)
        self.progressable = progressable

    @property
    def completed(self):
        return self.progressable.completed

    @property
    def task_progress(self):
        return self.progressable.task_progress

    async def __call__(self, *args):
        return await self.delegate(*args)

    def __repr__(self, *args, **kwargs):
        return repr(self.delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

class MultiClientRunner(Runner, Delegator):
    def __init__(self, runnable, name, client_extractor, context_manager_enabled=False):
        super().__init__(delegate=runnable)
        self.name = name
        self.client_extractor = client_extractor
        self.context_manager_enabled = context_manager_enabled

    async def __call__(self, *args):
        return await self.delegate(self.client_extractor(args[0]), *args[1:])

    def __repr__(self, *args, **kwargs):
        if self.context_manager_enabled:
            return "user-defined context-manager enabled runner for [%s]" % self.name
        else:
            return "user-defined runner for [%s]" % self.name

    async def __aenter__(self):
        if self.context_manager_enabled:
            await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context_manager_enabled:
            return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)
        else:
            return False


class AssertingRunner(Runner, Delegator):
    assertions_enabled = False

    def __init__(self, delegate):
        super().__init__(delegate=delegate)
        self.predicates = {
            ">": self.greater_than,
            ">=": self.greater_than_or_equal,
            "<": self.smaller_than,
            "<=": self.smaller_than_or_equal,
            "==": self.equal,
        }

    def greater_than(self, expected, actual):
        return actual > expected

    def greater_than_or_equal(self, expected, actual):
        return actual >= expected

    def smaller_than(self, expected, actual):
        return actual < expected

    def smaller_than_or_equal(self, expected, actual):
        return actual <= expected

    def equal(self, expected, actual):
        return actual == expected

    def check_assertion(self, op_name, assertion, properties):
        path = assertion["property"]
        predicate_name = assertion["condition"]
        expected_value = assertion["value"]
        actual_value = properties
        for k in path.split("."):
            actual_value = actual_value[k]
        predicate = self.predicates[predicate_name]
        success = predicate(expected_value, actual_value)
        if not success:
            if op_name:
                msg = f"Expected [{path}] in [{op_name}] to be {predicate_name} [{expected_value}] but was [{actual_value}]."
            else:
                msg = f"Expected [{path}] to be {predicate_name} [{expected_value}] but was [{actual_value}]."

            raise exceptions.BenchmarkTaskAssertionError(msg)

    async def __call__(self, *args):
        params = args[1]
        return_value = await self.delegate(*args)
        if AssertingRunner.assertions_enabled and "assertions" in params:
            op_name = params.get("name")
            if isinstance(return_value, dict):
                for assertion in params["assertions"]:
                    self.check_assertion(op_name, assertion, return_value)
            else:
                self.logger.debug("Skipping assertion check in [%s] as [%s] does not return a dict.",
                                  op_name, repr(self.delegate))
        return return_value

    def __repr__(self, *args, **kwargs):
        return repr(self.delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

class NoCompletion(Runner, Delegator):
    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    @property
    def completed(self):
        return None

    @property
    def task_progress(self):
        return None

    async def __call__(self, *args):
        return await self.delegate(*args)

    def __repr__(self, *args, **kwargs):
        return repr(self.delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

# === utility functions ===
def time_func(func):
    async def advised(*args, **kwargs):
        request_context_holder.on_client_request_start()
        try:
            response = await func(*args, **kwargs)
            return response
        finally:
            request_context_holder.on_client_request_end()
    return advised

def mandatory(params, key, op):
    try:
        return params[key]
    except KeyError:
        raise exceptions.DataError(
            f"Parameter source for operation '{str(op)}' did not provide the mandatory parameter '{key}'. "
            f"Add it to your parameter source and try again.")

def escape(v):
    """
    Escapes values so they can be used as query parameters

    :param v: The raw value. May be None.
    :return: The escaped value.
    """
    if v is None:
        return None
    elif isinstance(v, bool):
        return str(v).lower()
    else:
        return str(v)

# TODO: remove and use https://docs.python.org/3/library/stdtypes.html#str.removeprefix
#  once Python 3.9 becomes the minimum version
def remove_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    return string

def unwrap(runner):
    """
    Unwraps all delegators until the actual runner.

    :param runner: An arbitrarily nested chain of delegators around a runner.
    :return: The innermost runner.
    """
    delegate = getattr(runner, "delegate", None)
    if delegate:
        return unwrap(delegate)
    else:
        return runner

def _single_cluster_runner(runnable, name, context_manager_enabled=False):
    # only pass the default ES client
    return MultiClientRunner(runnable, name, lambda opensearch: opensearch["default"], context_manager_enabled)


def _multi_cluster_runner(runnable, name, context_manager_enabled=False):
    # pass all ES clients
    return MultiClientRunner(runnable, name, lambda opensearch: opensearch, context_manager_enabled)


def _with_assertions(delegate):
    return AssertingRunner(delegate)


def _with_completion(delegate):
    unwrapped_runner = unwrap(delegate)
    if hasattr(unwrapped_runner, "completed") and hasattr(unwrapped_runner, "task_progress"):
        return WithCompletion(delegate, unwrapped_runner)
    else:
        return NoCompletion(delegate)
