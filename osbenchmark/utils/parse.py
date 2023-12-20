# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
from osbenchmark.exceptions import ConfigurationError


def parse_string_parameter(key: str, params: dict, default: str = None) -> str:
    if key not in params or not params[key]:
        if default is not None:
            return default
        raise ConfigurationError(
            "Value cannot be None for param {}".format(key)
        )

    if isinstance(params[key], str):
        return params[key]

    raise ConfigurationError("Value must be a string for param {}".format(key))


def parse_int_parameter(key: str, params: dict, default: int = None) -> int:
    if key not in params:
        if default:
            return default
        raise ConfigurationError(
            "Value cannot be None for param {}".format(key)
        )

    if isinstance(params[key], int):
        return params[key]

    raise ConfigurationError("Value must be a int for param {}".format(key))


def parse_float_parameter(key: str, params: dict, default: float = None) -> float:
    if key not in params:
        if default:
            return default
        raise ConfigurationError(
            "Value cannot be None for param {}".format(key)
        )

    if isinstance(params[key], float):
        return params[key]

    raise ConfigurationError("Value must be a float for param {}".format(key))
