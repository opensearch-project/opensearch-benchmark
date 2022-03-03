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

import asyncio
import contextvars
import json
import logging
import random
import sys
import time
import types
import re
from collections import Counter, OrderedDict
from copy import deepcopy
from enum import Enum
from functools import total_ordering
from os.path import commonprefix
from typing import List, Optional
from io import BytesIO

import ijson

from osbenchmark import exceptions, workload

# Mapping from operation type to specific runner

__RUNNERS = {}


def register_default_runners():
    register_runner(workload.OperationType.Bulk, BulkIndex(), async_runner=True)
    register_runner(workload.OperationType.ForceMerge, ForceMerge(), async_runner=True)
    register_runner(workload.OperationType.IndexStats, Retry(IndicesStats()), async_runner=True)
    register_runner(workload.OperationType.NodeStats, NodeStats(), async_runner=True)
    register_runner(workload.OperationType.Search, Query(), async_runner=True)
    register_runner(workload.OperationType.PaginatedSearch, Query(), async_runner=True)
    register_runner(workload.OperationType.ScrollSearch, Query(), async_runner=True)
    register_runner(workload.OperationType.RawRequest, RawRequest(), async_runner=True)
    register_runner(workload.OperationType.Composite, Composite(), async_runner=True)
    register_runner(workload.OperationType.SubmitAsyncSearch, SubmitAsyncSearch(), async_runner=True)
    register_runner(workload.OperationType.GetAsyncSearch, Retry(GetAsyncSearch(), retry_until_success=True), async_runner=True)
    register_runner(workload.OperationType.DeleteAsyncSearch, DeleteAsyncSearch(), async_runner=True)
    register_runner(workload.OperationType.OpenPointInTime, OpenPointInTime(), async_runner=True)
    register_runner(workload.OperationType.ClosePointInTime, ClosePointInTime(), async_runner=True)

    # This is an administrative operation but there is no need for a retry here as we don't issue a request
    register_runner(workload.OperationType.Sleep, Sleep(), async_runner=True)
    # these requests should not be retried as they are not idempotent
    register_runner(workload.OperationType.CreateSnapshot, CreateSnapshot(), async_runner=True)
    register_runner(workload.OperationType.RestoreSnapshot, RestoreSnapshot(), async_runner=True)
    # We treat the following as administrative commands and thus already start to wrap them in a retry.
    register_runner(workload.OperationType.ClusterHealth, Retry(ClusterHealth()), async_runner=True)
    register_runner(workload.OperationType.PutPipeline, Retry(PutPipeline()), async_runner=True)
    register_runner(workload.OperationType.Refresh, Retry(Refresh()), async_runner=True)
    register_runner(workload.OperationType.CreateIndex, Retry(CreateIndex()), async_runner=True)
    register_runner(workload.OperationType.DeleteIndex, Retry(DeleteIndex()), async_runner=True)
    register_runner(workload.OperationType.CreateComponentTemplate, Retry(CreateComponentTemplate()), async_runner=True)
    register_runner(workload.OperationType.DeleteComponentTemplate, Retry(DeleteComponentTemplate()), async_runner=True)
    register_runner(workload.OperationType.CreateComposableTemplate, Retry(CreateComposableTemplate()), async_runner=True)
    register_runner(workload.OperationType.DeleteComposableTemplate, Retry(DeleteComposableTemplate()), async_runner=True)
    register_runner(workload.OperationType.CreateDataStream, Retry(CreateDataStream()), async_runner=True)
    register_runner(workload.OperationType.DeleteDataStream, Retry(DeleteDataStream()), async_runner=True)
    register_runner(workload.OperationType.CreateIndexTemplate, Retry(CreateIndexTemplate()), async_runner=True)
    register_runner(workload.OperationType.DeleteIndexTemplate, Retry(DeleteIndexTemplate()), async_runner=True)
    register_runner(workload.OperationType.ShrinkIndex, Retry(ShrinkIndex()), async_runner=True)
    register_runner(workload.OperationType.DeleteSnapshotRepository, Retry(DeleteSnapshotRepository()), async_runner=True)
    register_runner(workload.OperationType.CreateSnapshotRepository, Retry(CreateSnapshotRepository()), async_runner=True)
    register_runner(workload.OperationType.WaitForSnapshotCreate, Retry(WaitForSnapshotCreate()), async_runner=True)
    register_runner(workload.OperationType.WaitForRecovery, Retry(IndicesRecovery()), async_runner=True)
    register_runner(workload.OperationType.PutSettings, Retry(PutSettings()), async_runner=True)
    register_runner(workload.OperationType.CreateTransform, Retry(CreateTransform()), async_runner=True)
    register_runner(workload.OperationType.StartTransform, Retry(StartTransform()), async_runner=True)
    register_runner(workload.OperationType.WaitForTransform, Retry(WaitForTransform()), async_runner=True)
    register_runner(workload.OperationType.DeleteTransform, Retry(DeleteTransform()), async_runner=True)


def runner_for(operation_type):
    try:
        return __RUNNERS[operation_type]
    except KeyError:
        raise exceptions.BenchmarkError("No runner available for operation type [%s]" % operation_type)


def enable_assertions(enabled):
    """
    Changes whether assertions are enabled. The status changes for all tasks that are executed after this call.

    :param enabled: ``True`` to enable assertions, ``False`` to disable them.
    """
    AssertingRunner.assertions_enabled = enabled


def register_runner(operation_type, runner, **kwargs):
    logger = logging.getLogger(__name__)
    async_runner = kwargs.get("async_runner", False)
    if isinstance(operation_type, workload.OperationType):
        operation_type = operation_type.to_hyphenated_string()

    if not async_runner:
        raise exceptions.BenchmarkAssertionError(
            "Runner [{}] must be implemented as async runner and registered with async_runner=True.".format(str(runner)))

    if getattr(runner, "multi_cluster", False):
        if "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Registering runner object [%s] for [%s].", str(runner), str(operation_type))
            cluster_aware_runner = _multi_cluster_runner(runner, str(runner), context_manager_enabled=True)
        else:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug("Registering context-manager capable runner object [%s] for [%s].", str(runner), str(operation_type))
            cluster_aware_runner = _multi_cluster_runner(runner, str(runner))
    # we'd rather use callable() but this will erroneously also classify a class as callable...
    elif isinstance(runner, types.FunctionType):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering runner function [%s] for [%s].", str(runner), str(operation_type))
        cluster_aware_runner = _single_cluster_runner(runner, runner.__name__)
    elif "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering context-manager capable runner object [%s] for [%s].", str(runner), str(operation_type))
        cluster_aware_runner = _single_cluster_runner(runner, str(runner), context_manager_enabled=True)
    else:
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("Registering runner object [%s] for [%s].", str(runner), str(operation_type))
        cluster_aware_runner = _single_cluster_runner(runner, str(runner))

    __RUNNERS[operation_type] = _with_completion(_with_assertions(cluster_aware_runner))


# Only intended for unit-testing!
def remove_runner(operation_type):
    del __RUNNERS[operation_type]


class Runner:
    """
    Base class for all operations against OpenSearch.
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
        # map of API kwargs to Benchmark config parameters
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


class Delegator:
    """
    Mixin to unify delegate handling
    """
    def __init__(self, delegate, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delegate = delegate


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
    if hasattr(unwrapped_runner, "completed") and hasattr(unwrapped_runner, "percent_completed"):
        return WithCompletion(delegate, unwrapped_runner)
    else:
        return NoCompletion(delegate)


class NoCompletion(Runner, Delegator):
    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    @property
    def completed(self):
        return None

    @property
    def percent_completed(self):
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


class WithCompletion(Runner, Delegator):
    def __init__(self, delegate, progressable):
        super().__init__(delegate=delegate)
        self.progressable = progressable

    @property
    def completed(self):
        return self.progressable.completed

    @property
    def percent_completed(self):
        return self.progressable.percent_completed

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


def mandatory(params, key, op):
    try:
        return params[key]
    except KeyError:
        raise exceptions.DataError(
            f"Parameter source for operation '{str(op)}' did not provide the mandatory parameter '{key}'. "
            f"Add it to your parameter source and try again.")


# TODO: remove and use https://docs.python.org/3/library/stdtypes.html#str.removeprefix
#  once Python 3.9 becomes the minimum version
def remove_prefix(string, prefix):
    if string.startswith(prefix):
        return string[len(prefix):]
    return string


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


class BulkIndex(Runner):
    """
    Bulk indexes the given documents.
    """

    async def __call__(self, opensearch, params):
        """
        Runs one bulk indexing operation.

        :param opensearch: The OpenSearch client.
        :param params: A hash with all parameters. See below for details.
        :return: A hash with meta data for this bulk operation. See below for details.

        It expects a parameter dict with the following mandatory keys:

        * ``body``: containing all documents for the current bulk request.
        * ``bulk-size``: An indication of the bulk size denoted in ``unit``.
        * ``unit``: The name of the unit in which the bulk size is provided.
        * ``action_metadata_present``: if ``True``, assume that an action and metadata line is present (meaning only half of the lines
        contain actual documents to index)
        * ``index``: The name of the affected index in case ``action_metadata_present`` is ``False``.
        * ``type``: The name of the affected type in case ``action_metadata_present`` is ``False``.

        The following keys are optional:

        * ``pipeline``: If present, runs the the specified ingest pipeline for this bulk.
        * ``detailed-results``: If ``True``, the runner will analyze the response and add detailed meta-data. Defaults to ``False``. Note
        that this has a very significant impact on performance and will very
        likely cause a bottleneck in the benchmark worker_coordinator so please
        be very cautious enabling this feature. Our own measurements have shown a median overhead of several thousand times (execution time
         is in the single digit microsecond range when this feature is disabled and in the single digit millisecond range when this feature
         is enabled; numbers based on a bulk size of 500 elements and no errors). For details please refer to the respective benchmarks
         in ``benchmarks/worker_coordinator``.
        * ``request-timeout``: a non-negative float indicating the client-side timeout for the operation.  If not present, defaults to
         ``None`` and potentially falls back to the global timeout setting.
        """
        detailed_results = params.get("detailed-results", False)
        api_kwargs = self._default_kw_params(params)

        bulk_params = {}
        if "pipeline" in params:
            bulk_params["pipeline"] = params["pipeline"]

        with_action_metadata = mandatory(params, "action-metadata-present", self)
        bulk_size = mandatory(params, "bulk-size", self)
        unit = mandatory(params, "unit", self)
        # parse responses lazily in the standard case - responses might be large thus parsing skews results and if no
        # errors have occurred we only need a small amount of information from the potentially large response.
        if not detailed_results:
            opensearch.return_raw_response()

        if with_action_metadata:
            api_kwargs.pop("index", None)
            # only half of the lines are documents
            response = await opensearch.bulk(params=bulk_params, **api_kwargs)
        else:
            response = await opensearch.bulk(doc_type=params.get("type"), params=bulk_params, **api_kwargs)

        stats = self.detailed_stats(params, response) if detailed_results else self.simple_stats(bulk_size, unit, response)

        meta_data = {
            "index": params.get("index"),
            "weight": bulk_size,
            "unit": unit,
        }
        meta_data.update(stats)
        if not stats["success"]:
            meta_data["error-type"] = "bulk"
        return meta_data

    def detailed_stats(self, params, response):
        ops = {}
        shards_histogram = OrderedDict()
        bulk_error_count = 0
        bulk_success_count = 0
        error_details = set()
        bulk_request_size_bytes = 0
        total_document_size_bytes = 0
        with_action_metadata = mandatory(params, "action-metadata-present", self)

        if isinstance(params["body"], str):
            bulk_lines = params["body"].split("\n")
        elif isinstance(params["body"], list):
            bulk_lines = params["body"]
        else:
            raise exceptions.DataError("bulk body is neither string nor list")

        for line_number, data in enumerate(bulk_lines):
            line_size = len(data.encode('utf-8'))
            if with_action_metadata:
                if line_number % 2 == 1:
                    total_document_size_bytes += line_size
            else:
                total_document_size_bytes += line_size

            bulk_request_size_bytes += line_size

        for item in response["items"]:
            # there is only one (top-level) item
            op, data = next(iter(item.items()))
            if op not in ops:
                ops[op] = Counter()
            ops[op]["item-count"] += 1
            if "result" in data:
                ops[op][data["result"]] += 1

            if "_shards" in data:
                s = data["_shards"]
                sk = "%d-%d-%d" % (s["total"], s["successful"], s["failed"])
                if sk not in shards_histogram:
                    shards_histogram[sk] = {
                        "item-count": 0,
                        "shards": s
                    }
                shards_histogram[sk]["item-count"] += 1
            if data["status"] > 299 or ("_shards" in data and data["_shards"]["failed"] > 0):
                bulk_error_count += 1
                self.extract_error_details(error_details, data)
            else:
                bulk_success_count += 1
        stats = {
            "took": response.get("took"),
            "success": bulk_error_count == 0,
            "success-count": bulk_success_count,
            "error-count": bulk_error_count,
            "ops": ops,
            "shards_histogram": list(shards_histogram.values()),
            "bulk-request-size-bytes": bulk_request_size_bytes,
            "total-document-size-bytes": total_document_size_bytes
        }
        if bulk_error_count > 0:
            stats["error-type"] = "bulk"
            stats["error-description"] = self.error_description(error_details)
        if "ingest_took" in response:
            stats["ingest_took"] = response["ingest_took"]

        return stats

    def simple_stats(self, bulk_size, unit, response):
        bulk_success_count = bulk_size if unit == "docs" else None
        bulk_error_count = 0
        error_details = set()
        # parse lazily on the fast path
        props = parse(response, ["errors", "took"])

        if props.get("errors", False):
            # determine success count regardless of unit because we need to iterate through all items anyway
            bulk_success_count = 0
            # Reparse fully in case of errors - this will be slower
            parsed_response = json.loads(response.getvalue())
            for item in parsed_response["items"]:
                data = next(iter(item.values()))
                if data["status"] > 299 or ('_shards' in data and data["_shards"]["failed"] > 0):
                    bulk_error_count += 1
                    self.extract_error_details(error_details, data)
                else:
                    bulk_success_count += 1
        stats = {
            "took": props.get("took"),
            "success": bulk_error_count == 0,
            "success-count": bulk_success_count,
            "error-count": bulk_error_count
        }
        if bulk_error_count > 0:
            stats["error-type"] = "bulk"
            stats["error-description"] = self.error_description(error_details)
        return stats

    def extract_error_details(self, error_details, data):
        error_data = data.get("error", {})
        error_reason = error_data.get("reason") if isinstance(error_data, dict) else str(error_data)
        if error_data:
            error_details.add((data["status"], error_reason))
        else:
            error_details.add((data["status"], None))

    def error_description(self, error_details):
        error_description = ""
        for status, reason in error_details:
            if reason:
                error_description += "HTTP status: %s, message: %s" % (str(status), reason)
            else:
                error_description += "HTTP status: %s" % str(status)
        return error_description

    def __repr__(self, *args, **kwargs):
        return "bulk-index"


class ForceMerge(Runner):
    """
    Runs a force merge operation against OpenSearch.
    """

    async def __call__(self, opensearch, params):
        # pylint: disable=import-outside-toplevel
        import opensearchpy
        max_num_segments = params.get("max-num-segments")
        mode = params.get("mode")
        merge_params = self._default_kw_params(params)
        if max_num_segments:
            merge_params["max_num_segments"] = max_num_segments
        if mode == "polling":
            complete = False
            try:
                await opensearch.indices.forcemerge(**merge_params)
                complete = True
            except opensearchpy.ConnectionTimeout:
                pass
            while not complete:
                await asyncio.sleep(params.get("poll-period"))
                tasks = await opensearch.tasks.list(params={"actions": "indices:admin/forcemerge"})
                if len(tasks["nodes"]) == 0:
                    # empty nodes response indicates no tasks
                    complete = True
        else:
            await opensearch.indices.forcemerge(**merge_params)

    def __repr__(self, *args, **kwargs):
        return "force-merge"


class IndicesStats(Runner):
    """
    Gather index stats for all indices.
    """

    def _get(self, v, path):
        if v is None:
            return None
        elif len(path) == 1:
            return v.get(path[0])
        else:
            return self._get(v.get(path[0]), path[1:])

    def _safe_string(self, v):
        return str(v) if v is not None else None

    async def __call__(self, opensearch, params):
        api_kwargs = self._default_kw_params(params)
        index = api_kwargs.pop("index", "_all")
        condition = params.get("condition")
        response = await opensearch.indices.stats(index=index, metric="_all", **api_kwargs)
        if condition:
            path = mandatory(condition, "path", repr(self))
            expected_value = mandatory(condition, "expected-value", repr(self))
            actual_value = self._get(response, path.split("."))
            return {
                "weight": 1,
                "unit": "ops",
                "condition": {
                    "path": path,
                    # avoid mapping issues in the ES metrics store by always rendering values as strings
                    "actual-value": self._safe_string(actual_value),
                    "expected-value": self._safe_string(expected_value)
                },
                # currently we only support "==" as a predicate but that might change in the future
                "success": actual_value == expected_value
            }
        else:
            return {
                "weight": 1,
                "unit": "ops",
                "success": True
            }

    def __repr__(self, *args, **kwargs):
        return "indices-stats"


class NodeStats(Runner):
    """
    Gather node stats for all nodes.
    """

    async def __call__(self, opensearch, params):
        request_timeout = params.get("request-timeout")
        await opensearch.nodes.stats(metric="_all", request_timeout=request_timeout)

    def __repr__(self, *args, **kwargs):
        return "node-stats"


def parse(text: BytesIO, props: List[str], lists: List[str] = None) -> dict:
    """
    Selectively parse the provided text as JSON extracting only the properties provided in ``props``. If ``lists`` is
    specified, this function determines whether the provided lists are empty (respective value will be ``True``) or
    contain elements (respective key will be ``False``).

    :param text: A text to parse.
    :param props: A mandatory list of property paths (separated by a dot character) for which to extract values.
    :param lists: An optional list of property paths to JSON lists in the provided text.
    :return: A dict containing all properties and lists that have been found in the provided text.
    """
    text.seek(0)
    parser = ijson.parse(text)
    parsed = {}
    parsed_lists = {}
    current_list = None
    expect_end_array = False
    try:
        for prefix, event, value in parser:
            if expect_end_array:
                # True if the list is empty, False otherwise
                parsed_lists[current_list] = event == "end_array"
                expect_end_array = False
            if prefix in props:
                parsed[prefix] = value
            elif lists is not None and prefix in lists and event == "start_array":
                current_list = prefix
                expect_end_array = True
            # found all necessary properties
            if len(parsed) == len(props) and (lists is None or len(parsed_lists) == len(lists)):
                break
    except ijson.IncompleteJSONError:
        # did not find all properties
        pass

    parsed.update(parsed_lists)
    return parsed


class Query(Runner):
    """
    Runs a request body search against OpenSearch.

    It expects at least the following keys in the `params` hash:

    * `index`: The index or indices against which to issue the query.
    * `type`: See `index`
    * `cache`: True iff the request cache should be used.
    * `body`: Query body

    The following parameters are optional:

    * `detailed-results` (default: ``False``): Records more detailed meta-data about queries. As it analyzes the
                                               corresponding response in more detail, this might incur additional
                                               overhead which can skew measurement results. This flag is ineffective
                                               for scroll queries (detailed meta-data are always returned).
    * ``request-timeout``: a non-negative float indicating the client-side timeout for the operation.  If not present,
                           defaults to ``None`` and potentially falls back to the global timeout setting.
    * `results-per-page`: Number of results to retrieve per page.  This maps to the Search API's ``size`` parameter, and
                           can be used for paginated and non-paginated searches.  Defaults to ``10``

    If the following parameters are present in addition, a paginated query will be issued:

    * `pages`: Number of pages to retrieve at most for this search. If a query yields fewer results than the specified
               number of pages we will terminate earlier.


    Returned meta data

    The following meta data are always returned:

    * ``weight``: operation-agnostic representation of the "weight" of an
                  operation (used internally by Benchmark for throughput calculation).
                  Always 1 for normal queries and the number of retrieved pages for scroll queries.
    * ``unit``: The unit in which to interpret ``weight``. Always "ops".
    * ``hits``: Total number of hits for this operation.
    * ``hits_relation``: whether ``hits`` is accurate (``eq``) or a lower bound of the actual hit count (``gte``).
    * ``timed_out``: Whether the search has timed out. For scroll queries, this flag is ``True`` if the flag was ``True`` for any of the
                     queries issued.

    For paginated queries we also return:

    * ``pages``: Total number of pages that have been retrieved.
    """

    def __init__(self):
        super().__init__()
        self._extractor = SearchAfterExtractor()

    async def __call__(self, opensearch, params):
        request_params, headers = self._transport_request_params(params)
        # Mandatory to ensure it is always provided. This is especially important when this runner is used in a
        # composite context where there is no actual parameter source and the entire request structure must be provided
        # by the composite's parameter source.
        index = mandatory(params, "index", self)
        body = mandatory(params, "body", self)
        size = params.get("results-per-page")
        if size:
            body["size"] = size
        detailed_results = params.get("detailed-results", False)
        encoding_header = self._query_headers(params)
        if encoding_header is not None:
            headers.update(encoding_header)
        cache = params.get("cache")
        if cache is not None:
            request_params["request_cache"] = str(cache).lower()
        if not bool(headers):
            # counter-intuitive but preserves prior behavior
            headers = None
        # disable eager response parsing - responses might be huge thus skewing results
        opensearch.return_raw_response()

        async def _search_after_query(opensearch, params):
            index = params.get("index", "_all")
            pit_op = params.get("with-point-in-time-from")
            results = {
                "unit": "pages",
                "success": True,
                "timed_out": False,
                "took": 0
            }
            if pit_op:
                # these are disallowed as they are encoded in the pit_id
                for item in ["index", "routing", "preference"]:
                    body.pop(item, None)
                index = None
            # explicitly convert to int to provoke an error otherwise
            total_pages = sys.maxsize if params.get("pages") == "all" else int(mandatory(params, "pages", self))
            for page in range(1, total_pages + 1):
                if pit_op:
                    pit_id = CompositeContext.get(pit_op)
                    body["pit"] = {"id": pit_id,
                                   "keep_alive": "1m" }

                response = await self._raw_search(
                    opensearch, doc_type=None, index=index, body=body.copy(),
                    params=request_params, headers=headers)
                parsed, last_sort = self._extractor(response, bool(pit_op), results.get("hits"))
                results["pages"] = page
                results["weight"] = page
                if results.get("hits") is None:
                    results["hits"] = parsed.get("hits.total.value")
                    results["hits_relation"] = parsed.get("hits.total.relation")
                results["took"] += parsed.get("took")
                # when this evaluates to True, keep it for the final result
                if not results["timed_out"]:
                    results["timed_out"] = parsed.get("timed_out")
                if pit_op:
                    # per the documentation the response pit id is most up-to-date
                    CompositeContext.put(pit_op, parsed.get("pit_id"))

                if results.get("hits") / size > page:
                    body["search_after"] = last_sort
                else:
                    # body needs to be un-mutated for the next iteration (preferring to do this over a deepcopy at the start)
                    for item in ["pit", "search_after"]:
                        body.pop(item, None)
                    break

            return results

        async def _request_body_query(opensearch, params):
            doc_type = params.get("type")

            r = await self._raw_search(opensearch, doc_type, index, body, request_params, headers=headers)

            if detailed_results:
                props = parse(r, ["hits.total", "hits.total.value", "hits.total.relation", "timed_out", "took"])
                hits_total = props.get("hits.total.value", props.get("hits.total", 0))
                hits_relation = props.get("hits.total.relation", "eq")
                timed_out = props.get("timed_out", False)
                took = props.get("took", 0)

                return {
                    "weight": 1,
                    "unit": "ops",
                    "success": True,
                    "hits": hits_total,
                    "hits_relation": hits_relation,
                    "timed_out": timed_out,
                    "took": took
                }
            else:
                return {
                    "weight": 1,
                    "unit": "ops",
                    "success": True
                }

        async def _scroll_query(opensearch, params):
            hits = 0
            hits_relation = None
            timed_out = False
            took = 0
            retrieved_pages = 0
            scroll_id = None
            # explicitly convert to int to provoke an error otherwise
            total_pages = sys.maxsize if params.get("pages") == "all" else int(mandatory(params, "pages", self))
            try:
                for page in range(total_pages):
                    if page == 0:
                        sort = "_doc"
                        scroll = "10s"
                        doc_type = params.get("type")
                        params = request_params.copy()
                        params["sort"] = sort
                        params["scroll"] = scroll
                        params["size"] = size
                        r = await self._raw_search(opensearch, doc_type, index, body, params, headers=headers)

                        props = parse(r, ["_scroll_id", "hits.total", "hits.total.value", "hits.total.relation",
                                          "timed_out", "took"], ["hits.hits"])
                        scroll_id = props.get("_scroll_id")
                        hits = props.get("hits.total.value", props.get("hits.total", 0))
                        hits_relation = props.get("hits.total.relation", "eq")
                        timed_out = props.get("timed_out", False)
                        took = props.get("took", 0)
                        all_results_collected = (size is not None and hits < size) or hits == 0
                    else:
                        r = await opensearch.transport.perform_request("GET", "/_search/scroll",
                                                               body={"scroll_id": scroll_id, "scroll": "10s"},
                                                               params=request_params,
                                                               headers=headers)
                        props = parse(r, ["timed_out", "took"], ["hits.hits"])
                        timed_out = timed_out or props.get("timed_out", False)
                        took += props.get("took", 0)
                        # is the list of hits empty?
                        all_results_collected = props.get("hits.hits", False)
                    retrieved_pages +=1
                    if all_results_collected:
                        break
            finally:
                if scroll_id:
                    # noinspection PyBroadException
                    try:
                        await opensearch.clear_scroll(body={"scroll_id": [scroll_id]})
                    except BaseException:
                        self.logger.exception("Could not clear scroll [%s]. This will lead to excessive resource usage in "
                                              "OpenSearch and will skew your benchmark results.", scroll_id)

            return {
                "weight": retrieved_pages,
                "pages": retrieved_pages,
                "hits": hits,
                "hits_relation": hits_relation,
                "unit": "pages",
                "timed_out": timed_out,
                "took": took
            }

        search_method = params.get("operation-type")
        if search_method == "paginated-search":
            return await _search_after_query(opensearch, params)
        elif search_method == "scroll-search":
            return await _scroll_query(opensearch, params)
        elif "pages" in params:
            logging.getLogger(__name__).warning("Invoking a scroll search with the 'search' operation is deprecated "
                                                "and will be removed in a future release. Use 'scroll-search' instead.")
            return await _scroll_query(opensearch, params)
        else:
            return await _request_body_query(opensearch, params)

    async def _raw_search(self, opensearch, doc_type, index, body, params, headers=None):
        components = []
        if index:
            components.append(index)
        if doc_type:
            components.append(doc_type)
        components.append("_search")
        path = "/".join(components)
        return await opensearch.transport.perform_request("GET", "/" + path, params=params, body=body, headers=headers)

    def _query_headers(self, params):
        # reduces overhead due to decompression of very large responses
        if params.get("response-compression-enabled", True):
            return None
        else:
            return {"Accept-Encoding": "identity"}

    def __repr__(self, *args, **kwargs):
        return "query"


class SearchAfterExtractor:
    def __init__(self):
        # extracts e.g. '[1609780186, "2"]' from '"sort": [1609780186, "2"]'
        self.sort_pattern = re.compile(r"sort\":([^\]]*])")

    def __call__(self, response: BytesIO, get_point_in_time: bool, hits_total: Optional[int]) -> (dict, List):
        # not a class member as we would want to mutate over the course of execution for efficiency
        properties = ["timed_out", "took"]
        if get_point_in_time:
            properties.append("pit_id")
        # we only need to parse these the first time, subsequent responses should have the same values
        if hits_total is None:
            properties.extend(["hits.total", "hits.total.value", "hits.total.relation"])

        parsed = parse(response, properties)

        if get_point_in_time and not parsed.get("pit_id"):
            raise exceptions.BenchmarkAssertionError("Paginated query failure: "
                                                 "pit_id was expected but not found in the response.")
        # standardize these before returning...
        parsed["hits.total.value"] = parsed.pop("hits.total.value", parsed.pop("hits.total", hits_total))
        parsed["hits.total.relation"] = parsed.get("hits.total.relation", "eq")

        return parsed, self._get_last_sort(response)

    def _get_last_sort(self, response):
        """
        Algorithm is based on findings from benchmarks/worker_coordinator/parsing_test.py. Potentially a huge time sink if changed.
        """
        response_str = response.getvalue().decode("UTF-8")
        index_of_last_sort = response_str.rfind('"sort"')
        last_sort_str = re.search(self.sort_pattern, response_str[index_of_last_sort::])
        if last_sort_str is not None:
            return json.loads(last_sort_str.group(1))
        else:
            return None


class ClusterHealth(Runner):
    """
    Get cluster health
    """

    async def __call__(self, opensearch, params):
        @total_ordering
        class ClusterHealthStatus(Enum):
            UNKNOWN = 0
            RED = 1
            YELLOW = 2
            GREEN = 3

            def __lt__(self, other):
                if self.__class__ is other.__class__:
                    # pylint: disable=comparison-with-callable
                    return self.value < other.value
                return NotImplemented

        def status(v):
            try:
                return ClusterHealthStatus[v.upper()]
            except (KeyError, AttributeError):
                return ClusterHealthStatus.UNKNOWN

        request_params = params.get("request-params", {})
        api_kw_params = self._default_kw_params(params)
        # by default, OpenSearch will not wait and thus we treat this as success
        expected_cluster_status = request_params.get("wait_for_status", str(ClusterHealthStatus.UNKNOWN))
        if "wait_for_no_relocating_shards" in request_params:
            expected_relocating_shards = 0
        else:
            # we're good with any count of relocating shards.
            expected_relocating_shards = sys.maxsize

        result = await opensearch.cluster.health(**api_kw_params)
        cluster_status = result["status"]
        relocating_shards = result["relocating_shards"]

        result = {
            "weight": 1,
            "unit": "ops",
            "success": status(cluster_status) >= status(expected_cluster_status) and relocating_shards <= expected_relocating_shards,
            "cluster-status": cluster_status,
            "relocating-shards": relocating_shards
        }
        self.logger.info("%s: expected status=[%s], actual status=[%s], relocating shards=[%d], success=[%s].",
                         repr(self), expected_cluster_status, cluster_status, relocating_shards, result["success"])
        return result

    def __repr__(self, *args, **kwargs):
        return "cluster-health"


class PutPipeline(Runner):
    async def __call__(self, opensearch, params):
        await opensearch.ingest.put_pipeline(id=mandatory(params, "id", self),
                                     body=mandatory(params, "body", self),
                                     master_timeout=params.get("master-timeout"),
                                     timeout=params.get("timeout"),
                                     )

    def __repr__(self, *args, **kwargs):
        return "put-pipeline"


class Refresh(Runner):
    async def __call__(self, opensearch, params):
        await opensearch.indices.refresh(index=params.get("index", "_all"))

    def __repr__(self, *args, **kwargs):
        return "refresh"


class CreateIndex(Runner):
    async def __call__(self, opensearch, params):
        indices = mandatory(params, "indices", self)
        api_params = self._default_kw_params(params)
        ## ignore invalid entries rather than erroring
        for term in ["index", "body"]:
            api_params.pop(term, None)
        for index, body in indices:
            await opensearch.indices.create(index=index, body=body, **api_params)
        return {
            "weight": len(indices),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "create-index"


class CreateDataStream(Runner):
    async def __call__(self, opensearch, params):
        data_streams = mandatory(params, "data-streams", self)
        request_params = mandatory(params, "request-params", self)
        for data_stream in data_streams:
            await opensearch.indices.create_data_stream(data_stream, params=request_params)
        return {
            "weight": len(data_streams),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "create-data-stream"


class DeleteIndex(Runner):
    async def __call__(self, opensearch, params):
        ops = 0

        indices = mandatory(params, "indices", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})

        for index_name in indices:
            if not only_if_exists:
                await opensearch.indices.delete(index=index_name, params=request_params)
                ops += 1
            elif only_if_exists and await opensearch.indices.exists(index=index_name):
                self.logger.info("Index [%s] already exists. Deleting it.", index_name)
                await opensearch.indices.delete(index=index_name, params=request_params)
                ops += 1

        return {
            "weight": ops,
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "delete-index"


class DeleteDataStream(Runner):
    async def __call__(self, opensearch, params):
        ops = 0

        data_streams = mandatory(params, "data-streams", self)
        only_if_exists = mandatory(params, "only-if-exists", self)
        request_params = mandatory(params, "request-params", self)

        for data_stream in data_streams:
            if not only_if_exists:
                await opensearch.indices.delete_data_stream(data_stream, ignore=[404], params=request_params)
                ops += 1
            elif only_if_exists and await opensearch.indices.exists(index=data_stream):
                self.logger.info("Data stream [%s] already exists. Deleting it.", data_stream)
                await opensearch.indices.delete_data_stream(data_stream, params=request_params)
                ops += 1

        return {
            "weight": ops,
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "delete-data-stream"


class CreateComponentTemplate(Runner):
    async def __call__(self, opensearch, params):
        templates = mandatory(params, "templates", self)
        request_params = mandatory(params, "request-params", self)
        for template, body in templates:
            await opensearch.cluster.put_component_template(name=template, body=body,
                                                    params=request_params)
        return {
            "weight": len(templates),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "create-component-template"


class DeleteComponentTemplate(Runner):
    async def __call__(self, opensearch, params):
        template_names = mandatory(params, "templates", self)
        only_if_exists = mandatory(params, "only-if-exists", self)
        request_params = mandatory(params, "request-params", self)

        async def _exists(name):
            # pylint: disable=import-outside-toplevel
            from opensearchpy.client import _make_path
            # currently not supported by client and hence custom request
            return await opensearch.transport.perform_request(
                "HEAD", _make_path("_component_template", name)
            )

        ops_count = 0
        for template_name in template_names:
            if not only_if_exists:
                await opensearch.cluster.delete_component_template(name=template_name, params=request_params, ignore=[404])
                ops_count += 1
            elif only_if_exists and await _exists(template_name):
                self.logger.info("Component Index template [%s] already exists. Deleting it.", template_name)
                await opensearch.cluster.delete_component_template(name=template_name, params=request_params)
                ops_count += 1
        return {
            "weight": ops_count,
            "unit": "ops",
            "success": True
        }


    def __repr__(self, *args, **kwargs):
        return "delete-component-template"


class CreateComposableTemplate(Runner):
    async def __call__(self, opensearch, params):
        templates = mandatory(params, "templates", self)
        request_params = mandatory(params, "request-params", self)
        for template, body in templates:
            await opensearch.cluster.put_index_template(name=template, body=body, params=request_params)

        return {
            "weight": len(templates),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "create-composable-template"


class DeleteComposableTemplate(Runner):
    async def __call__(self, opensearch, params):
        templates = mandatory(params, "templates", self)
        only_if_exists = mandatory(params, "only-if-exists", self)
        request_params = mandatory(params, "request-params", self)
        ops_count = 0

        for template_name, delete_matching_indices, index_pattern in templates:
            if not only_if_exists:
                await opensearch.indices.delete_index_template(name=template_name, params=request_params, ignore=[404])
                ops_count += 1
            elif only_if_exists and await opensearch.indices.exists_template(template_name):
                self.logger.info("Composable Index template [%s] already exists. Deleting it.", template_name)
                await opensearch.indices.delete_index_template(name=template_name, params=request_params)
                ops_count += 1
            # ensure that we do not provide an empty index pattern by accident
            if delete_matching_indices and index_pattern:
                await opensearch.indices.delete(index=index_pattern)
                ops_count += 1

        return {
            "weight": ops_count,
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "delete-composable-template"


class CreateIndexTemplate(Runner):
    async def __call__(self, opensearch, params):
        templates = mandatory(params, "templates", self)
        request_params = params.get("request-params", {})
        for template, body in templates:
            await opensearch.indices.put_template(name=template,
                                          body=body,
                                          params=request_params)
        return {
            "weight": len(templates),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "create-index-template"


class DeleteIndexTemplate(Runner):
    async def __call__(self, opensearch, params):
        template_names = mandatory(params, "templates", self)
        only_if_exists = params.get("only-if-exists", False)
        request_params = params.get("request-params", {})
        ops_count = 0

        for template_name, delete_matching_indices, index_pattern in template_names:
            if not only_if_exists:
                await opensearch.indices.delete_template(name=template_name, params=request_params)
                ops_count += 1
            elif only_if_exists and await opensearch.indices.exists_template(template_name):
                self.logger.info("Index template [%s] already exists. Deleting it.", template_name)
                await opensearch.indices.delete_template(name=template_name, params=request_params)
                ops_count += 1
            # ensure that we do not provide an empty index pattern by accident
            if delete_matching_indices and index_pattern:
                await opensearch.indices.delete(index=index_pattern)
                ops_count += 1

        return {
            "weight": ops_count,
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "delete-index-template"


class ShrinkIndex(Runner):
    def __init__(self):
        super().__init__()
        self.cluster_health = Retry(ClusterHealth())

    async def _wait_for(self, opensearch, idx, description):
        # wait a little bit before the first check
        await asyncio.sleep(3)
        result = await self.cluster_health(opensearch, params={
            "index": idx,
            "retries": sys.maxsize,
            "request-params": {
                "wait_for_no_relocating_shards": "true"
            }
        })
        if not result["success"]:
            raise exceptions.BenchmarkAssertionError("Failed to wait for [{}].".format(description))

    async def __call__(self, opensearch, params):
        source_index = mandatory(params, "source-index", self)
        source_indices_get = await opensearch.indices.get(source_index)
        source_indices = list(source_indices_get.keys())
        source_indices_stem = commonprefix(source_indices)

        target_index = mandatory(params, "target-index", self)

        # we need to inject additional settings so we better copy the body
        target_body = deepcopy(mandatory(params, "target-body", self))
        shrink_node = params.get("shrink-node")
        # Choose a random data node if none is specified
        if shrink_node:
            node_names = [shrink_node]
        else:
            node_names = []
            # choose a random data node
            node_info = await opensearch.nodes.info()
            for node in node_info["nodes"].values():
                if "data" in node["roles"]:
                    node_names.append(node["name"])
            if not node_names:
                raise exceptions.BenchmarkAssertionError("Could not choose a suitable shrink-node automatically. Specify it explicitly.")

        for source_index in source_indices:
            shrink_node = random.choice(node_names)
            self.logger.info("Using [%s] as shrink node.", shrink_node)
            self.logger.info("Preparing [%s] for shrinking.", source_index)

            # prepare index for shrinking
            await opensearch.indices.put_settings(index=source_index,
                                          body={
                                              "settings": {
                                                  "index.routing.allocation.require._name": shrink_node,
                                                  "index.blocks.write": "true"
                                              }
                                          },
                                          preserve_existing=True)

            self.logger.info("Waiting for relocation to finish for index [%s] ...", source_index)
            await self._wait_for(opensearch, source_index, f"shard relocation for index [{source_index}]")
            self.logger.info("Shrinking [%s] to [%s].", source_index, target_index)
            if "settings" not in target_body:
                target_body["settings"] = {}
            target_body["settings"]["index.routing.allocation.require._name"] = None
            target_body["settings"]["index.blocks.write"] = None
            # kick off the shrink operation
            index_suffix = remove_prefix(source_index, source_indices_stem)
            final_target_index = target_index if len(index_suffix) == 0 else target_index+index_suffix
            await opensearch.indices.shrink(index=source_index, target=final_target_index, body=target_body)

            self.logger.info("Waiting for shrink to finish for index [%s] ...", source_index)
            await self._wait_for(opensearch, final_target_index, f"shrink for index [{final_target_index}]")
            self.logger.info("Shrinking [%s] to [%s] has finished.", source_index, final_target_index)
        # ops_count is not really important for this operation...
        return {
            "weight": len(source_indices),
            "unit": "ops",
            "success": True
        }

    def __repr__(self, *args, **kwargs):
        return "shrink-index"


class RawRequest(Runner):
    async def __call__(self, opensearch, params):
        request_params, headers = self._transport_request_params(params)
        if "ignore" in params:
            request_params["ignore"] = params["ignore"]
        path = mandatory(params, "path", self)
        if not path.startswith("/"):
            self.logger.error("RawRequest failed. Path parameter: [%s] must begin with a '/'.", path)
            raise exceptions.BenchmarkAssertionError(f"RawRequest [{path}] failed. Path parameter must begin with a '/'.")
        if not bool(headers):
            #counter-intuitive, but preserves prior behavior
            headers = None

        await opensearch.transport.perform_request(method=params.get("method", "GET"),
                                           url=path,
                                           headers=headers,
                                           body=params.get("body"),
                                           params=request_params)

    def __repr__(self, *args, **kwargs):
        return "raw-request"


class Sleep(Runner):
    """
    Sleeps for the specified duration not issuing any request.
    """

    async def __call__(self, opensearch, params):
        opensearch.on_request_start()
        try:
            await asyncio.sleep(mandatory(params, "duration", "sleep"))
        finally:
            opensearch.on_request_end()

    def __repr__(self, *args, **kwargs):
        return "sleep"


class DeleteSnapshotRepository(Runner):
    """
    Deletes a snapshot repository
    """
    async def __call__(self, opensearch, params):
        await opensearch.snapshot.delete_repository(repository=mandatory(params, "repository", repr(self)))

    def __repr__(self, *args, **kwargs):
        return "delete-snapshot-repository"


class CreateSnapshotRepository(Runner):
    """
    Creates a new snapshot repository
    """
    async def __call__(self, opensearch, params):
        request_params = params.get("request-params", {})
        await opensearch.snapshot.create_repository(repository=mandatory(params, "repository", repr(self)),
                                            body=mandatory(params, "body", repr(self)),
                                            params=request_params)

    def __repr__(self, *args, **kwargs):
        return "create-snapshot-repository"


class CreateSnapshot(Runner):
    """
    Creates a new snapshot repository
    """
    async def __call__(self, opensearch, params):
        wait_for_completion = params.get("wait-for-completion", False)
        repository = mandatory(params, "repository", repr(self))
        snapshot = mandatory(params, "snapshot", repr(self))
        # just assert, gets set in _default_kw_params
        mandatory(params, "body", repr(self))
        api_kwargs = self._default_kw_params(params)
        await opensearch.snapshot.create(repository=repository,
                                 snapshot=snapshot,
                                 wait_for_completion=wait_for_completion,
                                 **api_kwargs)

    def __repr__(self, *args, **kwargs):
        return "create-snapshot"


class WaitForSnapshotCreate(Runner):
    async def __call__(self, opensearch, params):
        repository = mandatory(params, "repository", repr(self))
        snapshot = mandatory(params, "snapshot", repr(self))
        wait_period = params.get("completion-recheck-wait-period", 1)

        snapshot_done = False
        stats = {}

        while not snapshot_done:
            response = await opensearch.snapshot.status(repository=repository,
                                                snapshot=snapshot,
                                                ignore_unavailable=True)

            if "snapshots" in response:
                response_state = response["snapshots"][0]["state"]
                if response_state == "FAILED":
                    self.logger.error("Snapshot [%s] failed. Response:\n%s", snapshot, json.dumps(response, indent=2))
                    raise exceptions.BenchmarkAssertionError(f"Snapshot [{snapshot}] failed. Please check logs.")
                snapshot_done = response_state == "SUCCESS"
                stats = response["snapshots"][0]["stats"]

            if not snapshot_done:
                await asyncio.sleep(wait_period)

        size = stats["total"]["size_in_bytes"]
        file_count = stats["total"]["file_count"]
        start_time_in_millis = stats["start_time_in_millis"]
        duration_in_millis = stats["time_in_millis"]
        duration_in_seconds = duration_in_millis / 1000

        return {
            "weight": size,
            "unit": "byte",
            "success": True,
            "throughput": size / duration_in_seconds,
            "start_time_millis": start_time_in_millis,
            "stop_time_millis": start_time_in_millis + duration_in_millis,
            "duration": duration_in_millis,
            "file_count": file_count
        }

    def __repr__(self, *args, **kwargs):
        return "wait-for-snapshot-create"


class RestoreSnapshot(Runner):
    """
    Restores a snapshot from an already registered repository
    """
    async def __call__(self, opensearch, params):
        api_kwargs = self._default_kw_params(params)
        await opensearch.snapshot.restore(repository=mandatory(params, "repository", repr(self)),
                                  snapshot=mandatory(params, "snapshot", repr(self)),
                                  wait_for_completion=params.get("wait-for-completion", False),
                                  **api_kwargs)

    def __repr__(self, *args, **kwargs):
        return "restore-snapshot"


class IndicesRecovery(Runner):
    async def __call__(self, opensearch, params):
        index = mandatory(params, "index", repr(self))
        wait_period = params.get("completion-recheck-wait-period", 1)

        all_shards_done = False
        total_recovered = 0
        total_start_millis = sys.maxsize
        total_end_millis = 0

        # wait until recovery is done
        # The nesting level is ok here given the structure of the API response
        # pylint: disable=too-many-nested-blocks
        while not all_shards_done:
            response = await opensearch.indices.recovery(index=index)
            # This might happen if we happen to call the API before the next recovery is scheduled.
            if not response:
                self.logger.debug("Empty index recovery response for [%s].", index)
            else:
                # check whether all shards are done
                all_shards_done = True
                total_recovered = 0
                total_start_millis = sys.maxsize
                total_end_millis = 0
                for _, idx_data in response.items():
                    for _, shard_data in idx_data.items():
                        for shard in shard_data:
                            current_shard_done = shard["stage"] == "DONE"
                            all_shards_done = all_shards_done and current_shard_done
                            if current_shard_done:
                                total_start_millis = min(total_start_millis, shard["start_time_in_millis"])
                                total_end_millis = max(total_end_millis, shard["stop_time_in_millis"])
                                idx_size = shard["index"]["size"]
                                total_recovered += idx_size["recovered_in_bytes"]
                self.logger.debug("All shards done for [%s]: [%s].", index, all_shards_done)

            if not all_shards_done:
                await asyncio.sleep(wait_period)

        response_time_in_seconds = (total_end_millis - total_start_millis) / 1000
        return {
            "weight": total_recovered,
            "unit": "byte",
            "success": True,
            "throughput": total_recovered / response_time_in_seconds,
            "start_time_millis": total_start_millis,
            "stop_time_millis": total_end_millis
        }

    def __repr__(self, *args, **kwargs):
        return "wait-for-recovery"


class PutSettings(Runner):
    async def __call__(self, opensearch, params):
        await opensearch.cluster.put_settings(body=mandatory(params, "body", repr(self)))

    def __repr__(self, *args, **kwargs):
        return "put-settings"


class CreateTransform(Runner):
    async def __call__(self, opensearch, params):
        transform_id = mandatory(params, "transform-id", self)
        body = mandatory(params, "body", self)
        defer_validation = params.get("defer-validation", False)
        await opensearch.transform.put_transform(transform_id=transform_id, body=body, defer_validation=defer_validation)

    def __repr__(self, *args, **kwargs):
        return "create-transform"


class StartTransform(Runner):
    async def __call__(self, opensearch, params):
        transform_id = mandatory(params, "transform-id", self)
        timeout = params.get("timeout")

        await opensearch.transform.start_transform(transform_id=transform_id, timeout=timeout)

    def __repr__(self, *args, **kwargs):
        return "start-transform"


class WaitForTransform(Runner):
    """
    Wait for the transform until it reaches a certain checkpoint.
    """

    def __init__(self):
        super().__init__()
        self._completed = False
        self._percent_completed = 0.0
        self._start_time = None
        self._last_documents_processed = 0
        self._last_processing_time = 0

    @property
    def completed(self):
        return self._completed

    @property
    def percent_completed(self):
        return self._percent_completed

    async def __call__(self, opensearch, params):
        """
        stop the transform and wait until transform has finished return stats

        :param opensearch: The OpenSearch client.
        :param params: A hash with all parameters. See below for details.
        :return: A hash with stats from the run.

        It expects a parameter dict with the following mandatory keys:

        * ``transform-id``: the transform id to start, the transform must have been created upfront.

        The following keys are optional:
        * ``force``: forcefully stop a transform, default false
        * ``wait-for-checkpoint``: whether to wait until all data has been processed till the next checkpoint, default true
        * ``wait-for-completion``: whether to block until the transform has stopped, default true
        * ``transform-timeout``: overall runtime timeout of the transform in seconds, default 3600 (1h)
        * ``poll-interval``: how often transform stats are polled, used to set progress and check the state, default 0.5.
        """
        transform_id = mandatory(params, "transform-id", self)
        force = params.get("force", False)
        timeout = params.get("timeout")
        wait_for_completion = params.get("wait-for-completion", True)
        wait_for_checkpoint = params.get("wait-for-checkpoint", True)
        transform_timeout = params.get("transform-timeout", 60.0 * 60.0)
        poll_interval = params.get("poll-interval", 0.5)

        if not self._start_time:
            self._start_time = time.monotonic()
            await opensearch.transform.stop_transform(transform_id=transform_id,
                                              force=force,
                                              timeout=timeout,
                                              wait_for_completion=False,
                                              wait_for_checkpoint=wait_for_checkpoint)

        while True:
            stats_response = await opensearch.transform.get_transform_stats(transform_id=transform_id)
            state = stats_response["transforms"][0].get("state")
            transform_stats = stats_response["transforms"][0].get("stats", {})

            if (time.monotonic() - self._start_time) > transform_timeout:
                raise exceptions.BenchmarkAssertionError(
                    f"Transform [{transform_id}] timed out after [{transform_timeout}] seconds. "
                    "Please consider increasing the timeout in the workload.")

            if state == "failed":
                failure_reason = stats_response["transforms"][0].get("reason", "unknown")
                raise exceptions.BenchmarkAssertionError(
                    f"Transform [{transform_id}] failed with [{failure_reason}].")
            elif state == "stopped" or wait_for_completion is False:
                self._completed = True
                self._percent_completed = 1.0
            else:
                self._percent_completed = stats_response["transforms"][0].get("checkpointing", {}).get("next", {}).get(
                    "checkpoint_progress", {}).get("percent_complete", 0.0) / 100.0

            documents_processed = transform_stats.get("documents_processed", 0)
            processing_time = transform_stats.get("search_time_in_ms", 0)
            processing_time += transform_stats.get("processing_time_in_ms", 0)
            processing_time += transform_stats.get("index_time_in_ms", 0)
            documents_processed_delta = documents_processed - self._last_documents_processed
            processing_time_delta = processing_time - self._last_processing_time

            # only publish if we have enough data or transform has completed
            if self._completed or (documents_processed_delta > 5000 and processing_time_delta > 500):
                stats = {
                    "transform-id": transform_id,
                    "weight": transform_stats.get("documents_processed", 0),
                    "unit": "docs",
                    "success": True
                }

                throughput = 0
                if self._completed:
                    # take the overall throughput
                    if processing_time > 0:
                        throughput = documents_processed / processing_time * 1000
                elif processing_time_delta > 0:
                    throughput = documents_processed_delta / processing_time_delta * 1000

                stats["throughput"] = throughput

                self._last_documents_processed = documents_processed
                self._last_processing_time = processing_time
                return stats
            else:
                # sleep for a while, so stats is not called to often
                await asyncio.sleep(poll_interval)

    def __repr__(self, *args, **kwargs):
        return "wait-for-transform"


class DeleteTransform(Runner):
    async def __call__(self, opensearch, params):
        transform_id = mandatory(params, "transform-id", self)
        force = params.get("force", False)
        # we don't want to fail if a job does not exist, thus we ignore 404s.
        await opensearch.transform.delete_transform(transform_id=transform_id, force=force, ignore=[404])

    def __repr__(self, *args, **kwargs):
        return "delete-transform"


class SubmitAsyncSearch(Runner):
    async def __call__(self, opensearch, params):
        request_params = params.get("request-params", {})
        response = await opensearch.async_search.submit(body=mandatory(params, "body", self),
                                                index=params.get("index"),
                                                params=request_params)

        op_name = mandatory(params, "name", self)
        # id may be None if the operation has already returned
        search_id = response.get("id")
        CompositeContext.put(op_name, search_id)

    def __repr__(self, *args, **kwargs):
        return "submit-async-search"


def async_search_ids(op_names):
    subjects = [op_names] if isinstance(op_names, str) else op_names
    for subject in subjects:
        subject_id = CompositeContext.get(subject)
        # skip empty ids, searches have already completed
        if subject_id:
            yield subject_id, subject


class GetAsyncSearch(Runner):
    async def __call__(self, opensearch, params):
        success = True
        searches = mandatory(params, "retrieve-results-for", self)
        request_params = params.get("request-params", {})
        stats = {}
        for search_id, search in async_search_ids(searches):
            response = await opensearch.async_search.get(id=search_id,
                                                 params=request_params)
            is_running = response["is_running"]
            success = success and not is_running
            if not is_running:
                stats[search] = {
                    "hits": response["response"]["hits"]["total"]["value"],
                    "hits_relation": response["response"]["hits"]["total"]["relation"],
                    "timed_out": response["response"]["timed_out"],
                    "took": response["response"]["took"]
                }

        return {
            # only count completed searches - there is one key per search id in `stats`
            "weight": len(stats),
            "unit": "ops",
            "success": success,
            "stats": stats
        }

    def __repr__(self, *args, **kwargs):
        return "get-async-search"


class DeleteAsyncSearch(Runner):
    async def __call__(self, opensearch, params):
        searches = mandatory(params, "delete-results-for", self)
        for search_id, search in async_search_ids(searches):
            await opensearch.async_search.delete(id=search_id)
            CompositeContext.remove(search)

    def __repr__(self, *args, **kwargs):
        return "delete-async-search"


class OpenPointInTime(Runner):
    async def __call__(self, opensearch, params):
        op_name = mandatory(params, "name", self)
        index = mandatory(params, "index", self)
        keep_alive = params.get("keep-alive", "1m")
        response = await opensearch.open_point_in_time(index=index,
                                         params=params.get("request-params"),
                                         keep_alive=keep_alive)
        id = response.get("id")
        CompositeContext.put(op_name, id)

    def __repr__(self, *args, **kwargs):
        return "open-point-in-time"


class ClosePointInTime(Runner):
    async def __call__(self, opensearch, params):
        pit_op = mandatory(params, "with-point-in-time-from", self)
        pit_id = CompositeContext.get(pit_op)
        request_params = params.get("request-params", {})
        body = {"id": pit_id}
        await opensearch.close_point_in_time(body=body, params=request_params, headers=None)
        CompositeContext.remove(pit_op)

    def __repr__(self, *args, **kwargs):
        return "close-point-in-time"


class CompositeContext:
    ctx = contextvars.ContextVar("composite_context")

    def __init__(self):
        self.token = None

    async def __aenter__(self):
        self.token = CompositeContext.ctx.set({})
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        CompositeContext.ctx.reset(self.token)
        return False

    @staticmethod
    def put(key, value):
        CompositeContext._ctx()[key] = value

    @staticmethod
    def get(key):
        try:
            return CompositeContext._ctx()[key]
        except KeyError:
            raise KeyError(f"Unknown property [{key}]. Currently recognized "
                           f"properties are [{', '.join(CompositeContext._ctx().keys())}].") from None

    @staticmethod
    def remove(key):
        try:
            CompositeContext._ctx().pop(key)
        except KeyError:
            raise KeyError(f"Unknown property [{key}]. Currently recognized "
                           f"properties are [{', '.join(CompositeContext._ctx().keys())}].") from None

    @staticmethod
    def _ctx():
        try:
            return CompositeContext.ctx.get()
        except LookupError:
            raise exceptions.BenchmarkAssertionError("This operation is only allowed inside a composite operation.") from None


class Composite(Runner):
    """
    Executes a complex request structure which is measured by Benchmark as one composite operation.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.supported_op_types = [
            "open-point-in-time",
            "close-point-in-time",
            "search",
            "raw-request",
            "sleep",
            "submit-async-search",
            "get-async-search",
            "delete-async-search"
        ]

    async def run_stream(self, opensearch, stream, connection_limit):
        streams = []
        timings = []
        try:
            for item in stream:
                if "stream" in item:
                    streams.append(asyncio.create_task(self.run_stream(opensearch, item["stream"], connection_limit)))
                elif "operation-type" in item:
                    # consume all prior streams first
                    if streams:
                        streams_timings = await asyncio.gather(*streams)
                        for stream_timings in streams_timings:
                            timings += stream_timings
                        streams = []
                    op_type = item["operation-type"]
                    if op_type not in self.supported_op_types:
                        raise exceptions.BenchmarkAssertionError(
                            f"Unsupported operation-type [{op_type}]. Use one of [{', '.join(self.supported_op_types)}].")
                    runner = RequestTiming(runner_for(op_type))
                    async with connection_limit:
                        async with runner:
                            response = await runner({"default": opensearch}, item)
                            timing = response.get("dependent_timing") if response else None
                            if timing:
                                timings.append(timing)

                else:
                    raise exceptions.BenchmarkAssertionError("Requests structure must contain [stream] or [operation-type].")
        except BaseException:
            # stop all already created tasks in case of exceptions
            for s in streams:
                if not s.done():
                    s.cancel()
            raise

        # complete any outstanding streams
        if streams:
            streams_timings = await asyncio.gather(*streams)
            for stream_timings in streams_timings:
                timings += stream_timings
        return timings

    async def __call__(self, opensearch, params):
        requests = mandatory(params, "requests", self)
        max_connections = params.get("max-connections", sys.maxsize)
        async with CompositeContext():
            response = await self.run_stream(opensearch, requests, asyncio.BoundedSemaphore(max_connections))
        return {
            "weight": 1,
            "unit": "ops",
            "dependent_timing": response
        }

    def __repr__(self, *args, **kwargs):
        return "composite"


class RequestTiming(Runner, Delegator):
    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, opensearch, params):
        absolute_time = time.time()
        async with opensearch["default"].new_request_context() as request_context:
            return_value = await self.delegate(opensearch, params)
            if isinstance(return_value, tuple) and len(return_value) == 2:
                total_ops, total_ops_unit = return_value
                result = {
                    "weight": total_ops,
                    "unit": total_ops_unit,
                    "success": True
                }
            elif isinstance(return_value, dict):
                result = return_value
            else:
                result = {
                    "weight": 1,
                    "unit": "ops",
                    "success": True
                }

            start = request_context.request_start
            end = request_context.request_end
            result["dependent_timing"] = {
                "operation": params.get("name"),
                "operation-type": params.get("operation-type"),
                "absolute_time": absolute_time,
                "request_start": start,
                "request_end": end,
                "service_time": end - start
            }
        return result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)


# TODO: Allow to use this from (selected) regular runners and add user documentation.
# TODO: It would maybe be interesting to add meta-data on how many retries there were.
class Retry(Runner, Delegator):
    """
    This runner can be used as a wrapper around regular runners to retry operations.

    It defines the following parameters:

    * ``retries`` (optional, default 0): The number of times the operation is retried.
    * ``retry-until-success`` (optional, default False): Retries until the delegate returns a success. This will also
                              forcibly set ``retry-on-error`` to ``True``.
    * ``retry-wait-period`` (optional, default 0.5): The time in seconds to wait after an error.
    * ``retry-on-timeout`` (optional, default True): Whether to retry on connection timeout.
    * ``retry-on-error`` (optional, default False): Whether to retry on failure (i.e. the delegate
                         returns ``success == False``)
    """

    def __init__(self, delegate, retry_until_success=False):
        super().__init__(delegate=delegate)
        self.retry_until_success = retry_until_success

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, opensearch, params):
        # pylint: disable=import-outside-toplevel
        import opensearchpy
        import socket

        retry_until_success = params.get("retry-until-success", self.retry_until_success)
        if retry_until_success:
            max_attempts = sys.maxsize
            retry_on_error = True
        else:
            max_attempts = params.get("retries", 0) + 1
            retry_on_error = params.get("retry-on-error", False)
        sleep_time = params.get("retry-wait-period", 0.5)
        retry_on_timeout = params.get("retry-on-timeout", True)

        for attempt in range(max_attempts):
            last_attempt = attempt + 1 == max_attempts
            try:
                return_value = await self.delegate(opensearch, params)
                if last_attempt or not retry_on_error:
                    return return_value
                # we can determine success if and only if the runner returns a dict. Otherwise, we have to assume it was fine.
                elif isinstance(return_value, dict):
                    if return_value.get("success", True):
                        self.logger.debug("%s has returned successfully", repr(self.delegate))
                        return return_value
                    else:
                        self.logger.info("[%s] has returned with an error: %s. Retrying in [%.2f] seconds.",
                                         repr(self.delegate), return_value, sleep_time)
                        await asyncio.sleep(sleep_time)
                else:
                    return return_value
            except (socket.timeout, opensearchpy.exceptions.ConnectionError):
                if last_attempt or not retry_on_timeout:
                    raise
                else:
                    await asyncio.sleep(sleep_time)
            except opensearchpy.exceptions.TransportError as e:
                if last_attempt or not retry_on_timeout:
                    raise e
                elif e.status_code == 408:
                    self.logger.info("[%s] has timed out. Retrying in [%.2f] seconds.", repr(self.delegate), sleep_time)
                    await asyncio.sleep(sleep_time)
                else:
                    raise e

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __repr__(self, *args, **kwargs):
        return "retryable %s" % repr(self.delegate)
