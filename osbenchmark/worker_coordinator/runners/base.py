# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

"""
Base runner classes and utilities for OpenSearch Benchmark.

This module contains the core runner infrastructure that is shared
across all database-specific runner implementations.
"""

import logging
import types
from typing import Any, Dict, Optional

from osbenchmark import exceptions, workload
from osbenchmark.context import RequestContextHolder

# Global runner registry
__RUNNERS = {}

# Shared request context holder
request_context_holder = RequestContextHolder()


def mandatory(params, key, runner):
    """
    Get a mandatory parameter from the params dict.

    Args:
        params: Parameter dictionary
        key: Key to look up
        runner: Runner instance (for error messages)

    Returns:
        The parameter value

    Raises:
        BenchmarkAssertionError: If the key is missing
    """
    try:
        return params[key]
    except KeyError:
        raise exceptions.BenchmarkAssertionError(
            f"Parameter [{key}] is mandatory for [{runner}]"
        )


def register_runner(operation_type, runner, **kwargs):
    """
    Register a runner for an operation type.

    Args:
        operation_type: The operation type (string or OperationType enum)
        runner: The runner instance
        **kwargs: Additional options (async_runner must be True)
    """
    logger = logging.getLogger(__name__)
    async_runner = kwargs.get("async_runner", False)

    if isinstance(operation_type, workload.OperationType):
        operation_type = operation_type.to_hyphenated_string()

    if not async_runner:
        raise exceptions.BenchmarkAssertionError(
            f"Runner [{runner}] must be implemented as async runner and registered with async_runner=True."
        )

    if getattr(runner, "multi_cluster", False):
        if "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
            cluster_aware_runner = _multi_cluster_runner(runner, str(runner), context_manager_enabled=True)
        else:
            cluster_aware_runner = _multi_cluster_runner(runner, str(runner))
    elif isinstance(runner, types.FunctionType):
        cluster_aware_runner = _single_cluster_runner(runner, runner.__name__)
    elif "__aenter__" in dir(runner) and "__aexit__" in dir(runner):
        cluster_aware_runner = _single_cluster_runner(runner, str(runner), context_manager_enabled=True)
    else:
        cluster_aware_runner = _single_cluster_runner(runner, str(runner))

    __RUNNERS[operation_type] = _with_completion(_with_assertions(cluster_aware_runner))

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("Registered runner [%s] for operation type [%s]", str(runner), operation_type)


def runner_for(operation_type):
    """Get the registered runner for an operation type."""
    try:
        return __RUNNERS[operation_type]
    except KeyError:
        raise exceptions.BenchmarkError(f"No runner available for operation type [{operation_type}]")


def remove_runner(operation_type):
    """Remove a registered runner (for testing)."""
    del __RUNNERS[operation_type]


def enable_assertions(enabled):
    """Enable or disable assertions for all runners."""
    AssertingRunner.assertions_enabled = enabled


def time_func(func):
    """
    Decorator to wrap a function with request timing.

    Calls all four timing methods:
    - on_client_request_start/end: Marks the logical operation boundaries
    - on_request_start/end: Marks the actual request boundaries

    For no-op operations (like Vespa stubs), both pairs are called together.
    """
    async def advised(*args, **kwargs):
        request_context_holder.on_client_request_start()
        request_context_holder.on_request_start()
        try:
            response = await func(*args, **kwargs)
            return response
        finally:
            request_context_holder.on_request_end()
            request_context_holder.on_client_request_end()
    return advised


class Runner:
    """
    Base class for all benchmark operations.

    Subclasses must implement the __call__ method to perform the actual operation.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(__name__)

    async def __aenter__(self):
        return self

    async def __call__(self, client, params):
        """
        Run the benchmark operation.

        Args:
            client: Database client instance
            params: Operation parameters

        Returns:
            A dict with at least:
            - weight: Number of operations (default 1)
            - unit: Unit of measurement (default "ops")
            - success: Whether the operation succeeded
        """
        raise NotImplementedError("abstract operation")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

    def _default_kw_params(self, params):
        """Extract common API keyword parameters from params."""
        kw_dict = {
            "body": "body",
            "headers": "headers",
            "index": "index",
            "opaque_id": "opaque-id",
            "params": "request-params",
            "request_timeout": "request-timeout",
        }
        full_result = {k: params.get(v) for (k, v) in kw_dict.items()}
        return dict(filter(lambda kv: kv[1] is not None, full_result.items()))

    def _transport_request_params(self, params):
        """Extract transport-level request parameters."""
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
    """Mixin to unify delegate handling."""

    def __init__(self, delegate, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.delegate = delegate


def unwrap(runner):
    """Unwrap a delegating runner to get the underlying runner."""
    delegate = getattr(runner, "delegate", None)
    if delegate is not None:
        return unwrap(delegate)
    return runner


def _single_cluster_runner(runnable, name, context_manager_enabled=False):
    """Wrap a runner to extract the default cluster client."""
    return MultiClientRunner(runnable, name, lambda clients: clients["default"], context_manager_enabled)


def _multi_cluster_runner(runnable, name, context_manager_enabled=False):
    """Wrap a runner to pass all cluster clients."""
    return MultiClientRunner(runnable, name, lambda clients: clients, context_manager_enabled)


class MultiClientRunner(Runner, Delegator):
    """Runner wrapper that handles client extraction from the clients dict."""

    def __init__(self, runnable, name, client_extractor, context_manager_enabled=False):
        super().__init__(delegate=runnable)
        self.name = name
        self.client_extractor = client_extractor
        self.context_manager_enabled = context_manager_enabled

    async def __call__(self, *args):
        return await self.delegate(self.client_extractor(args[0]), *args[1:])

    def __repr__(self):
        if self.context_manager_enabled:
            return f"user-defined context-manager enabled runner for [{self.name}]"
        return f"user-defined runner for [{self.name}]"

    async def __aenter__(self):
        if self.context_manager_enabled:
            await self.delegate.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.context_manager_enabled:
            return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)
        return False


class AssertingRunner(Runner, Delegator):
    """Runner wrapper that validates assertions after execution."""

    assertions_enabled = False

    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, *args, **kwargs):
        return_value = await self.delegate(*args, **kwargs)
        if AssertingRunner.assertions_enabled and isinstance(return_value, dict):
            assertions = return_value.get("assertions")
            if assertions:
                self._check_assertions(assertions)
        return return_value

    def _check_assertions(self, assertions):
        for assertion in assertions:
            name = assertion.get("name", "Unknown")
            success = assertion.get("success", True)
            if not success:
                message = assertion.get("message", "Assertion failed")
                raise exceptions.BenchmarkAssertionError(f"Assertion [{name}] failed: {message}")

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    def __repr__(self):
        return f"asserting [{self.delegate}]"


def _with_assertions(delegate):
    """Wrap a runner with assertion checking."""
    return AssertingRunner(delegate)


class WithCompletion(Runner, Delegator):
    """Runner wrapper that tracks task completion."""

    def __init__(self, delegate, target):
        super().__init__(delegate=delegate)
        self.target = target

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, *args, **kwargs):
        return await self.delegate(*args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def completed(self):
        return self.target.completed

    @property
    def task_progress(self):
        return self.target.task_progress

    def __repr__(self):
        return repr(self.delegate)


class NoCompletion(Runner, Delegator):
    """Runner wrapper for runners without completion tracking."""

    def __init__(self, delegate):
        super().__init__(delegate=delegate)

    async def __aenter__(self):
        await self.delegate.__aenter__()
        return self

    async def __call__(self, *args, **kwargs):
        return await self.delegate(*args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.delegate.__aexit__(exc_type, exc_val, exc_tb)

    @property
    def completed(self):
        return None

    @property
    def task_progress(self):
        return None

    def __repr__(self):
        return repr(self.delegate)


def _with_completion(delegate):
    """Wrap a runner with completion tracking if supported."""
    unwrapped_runner = unwrap(delegate)
    if hasattr(unwrapped_runner, "completed") and hasattr(unwrapped_runner, "task_progress"):
        return WithCompletion(delegate, unwrapped_runner)
    return NoCompletion(delegate)
