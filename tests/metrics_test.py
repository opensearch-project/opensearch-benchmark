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
# pylint: disable=protected-access

import collections
import datetime
import json
import logging
import os
import random
import string
import tempfile
import unittest.mock as mock
import uuid
from unittest import TestCase

import opensearchpy.exceptions
import pytest

from osbenchmark import config, metrics, workload, exceptions, paths
from osbenchmark.metrics import GlobalStatsCalculator
from osbenchmark.workload import Task, Operation, Procedure, Workload

AWS_ACCESS_KEY_ID_LENGTH = 12
AWS_SECRET_ACCESS_KEY_LENGTH = 40
AWS_SESSION_TOKEN_LENGTH = 752

class MockClientFactory:
    def __init__(self, cfg):
        self._es = mock.create_autospec(metrics.OsClient)

    def create(self):
        return self._es


class DummyIndexTemplateProvider:
    def __init__(self, cfg):
        pass

    def metrics_template(self):
        return "metrics-test-template"

    def test_executions_template(self):
        return "test-executions-test-template"

    def results_template(self):
        return "results-test-template"


class StaticClock:
    NOW = 1453362707

    @staticmethod
    def now():
        return StaticClock.NOW

    @staticmethod
    def stop_watch():
        return StaticStopWatch()


class StaticStopWatch:
    def start(self):
        pass

    def stop(self):
        pass

    def split_time(self):
        return 0

    def total_time(self):
        return 0


class TransportErrors:
    err_return_codes = {502: "Bad Gateway",
                        503: "Service Unavailable",
                        504: "Gateway Timeout",
                        429: "Too Many Requests"}

    def __init__(self, max_err_responses=10):
        self.max_err_responses = max_err_responses
        # allow duplicates in list of error codes
        self.rnd_err_codes = [
            random.choice(list(TransportErrors.err_return_codes))
            for _ in range(self.max_err_responses)
        ]

    @property
    def code_list(self):
        return self.rnd_err_codes

    @property
    def side_effects(self):
        side_effect_list = [
            opensearchpy.exceptions.TransportError(rnd_code, TransportErrors.err_return_codes[rnd_code])
            for rnd_code in self.rnd_err_codes
        ]
        side_effect_list.append("success")

        return side_effect_list


class ExtractUserTagsTests(TestCase):
    def test_no_tags_returns_empty_dict(self):
        cfg = config.Config()
        self.assertEqual(0, len(metrics.extract_user_tags_from_config(cfg)))

    def test_missing_comma_raises_error(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "test_execution", "user.tag", "invalid")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            metrics.extract_user_tags_from_config(cfg)
        self.assertEqual("User tag keys and values have to separated by a ':'. Invalid value [invalid]", ctx.exception.args[0])

    def test_missing_value_raises_error(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "test_execution", "user.tag", "invalid1,invalid2")
        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            metrics.extract_user_tags_from_config(cfg)
        self.assertEqual("User tag keys and values have to separated by a ':'. Invalid value [invalid1,invalid2]", ctx.exception.args[0])

    def test_extracts_proper_user_tags(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "test_execution", "user.tag", "os:Linux,cpu:ARM")
        self.assertDictEqual({"os": "Linux", "cpu": "ARM"}, metrics.extract_user_tags_from_config(cfg))


class OsClientTests(TestCase):
    class TransportMock:
        def __init__(self, hosts):
            self.hosts = hosts

    class ClientMock:
        def __init__(self, hosts):
            self.transport = OsClientTests.TransportMock(hosts)

    def test_config_opts_parsing_with_none(self):
        # Tries parsing but fails because it does not detect password from ENV or Config
        self.config_opts_parsing(None)

    @mock.patch("osbenchmark.client.OsClientFactory")
    def test_config_opts_parsing_with_config(self, client_OsClientfactory):
        config_opts = self.config_opts_parsing("config")

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "basic_auth_user": config_opts["_datastore_user"],
            "basic_auth_password": config_opts["_datastore_password"],
            "verify_certs": config_opts["_datastore_verify_certs"]
        }

        client_OsClientfactory.assert_called_with(
            hosts=[{"host": config_opts["_datastore_host"], "port": config_opts["_datastore_port"]}],
            client_options=expected_client_options
        )

    @mock.patch("osbenchmark.client.OsClientFactory")
    def test_config_opts_parsing_with_env(self, client_OsClientfactory):
        config_opts = self.config_opts_parsing("environment")

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "basic_auth_user": config_opts["_datastore_user"],
            "basic_auth_password": config_opts["_datastore_password"],
            "verify_certs": config_opts["_datastore_verify_certs"]
        }

        client_OsClientfactory.assert_called_with(
            hosts=[{"host": config_opts["_datastore_host"], "port": config_opts["_datastore_port"]}],
            client_options=expected_client_options
        )

    @mock.patch("osbenchmark.client.OsClientFactory")
    def test_config_opts_parsing_aws_creds_with_config(self, client_OsClientfactory):
        # verify config parsing fails when configuration source for aws creds is set to None
        self.config_opts_parsing_aws_creds("None")

        # verify config parsing fails when configuration for aws creds is missing a required config key
        override_config = {
            "datastore.aws_access_key_id": None
        }
        self.config_opts_parsing_aws_creds("config", override_datastore=override_config)

        # verify config parsing is successful when all required parameters are present
        config_opts = self.config_opts_parsing_aws_creds("config")

        expected_client_options = {
            "use_ssl": True,
            "timeout": 120,
            "amazon_aws_log_in": 'client_option',
            "aws_access_key_id": config_opts["_datastore_aws_access_key_id"],
            "aws_secret_access_key": config_opts["_datastore_aws_secret_access_key"],
            "service": config_opts["_datastore_aws_service"],
            "region": config_opts["_datastore_aws_region"],
            "verify_certs": config_opts["_datastore_verify_certs"]
        }

        client_OsClientfactory.assert_called_with(
            hosts=[{"host": config_opts["_datastore_host"], "port": config_opts["_datastore_port"]}],
            client_options=expected_client_options
        )

    def test_metrics_client_creation_with_aws_creds(self):
        try:
            self.config_opts_parsing_aws_creds("config")
        except Exception:
            assert False, "Metrics client creation should not fail"

    @mock.patch("osbenchmark.client.OsClientFactory")
    def test_config_opts_parsing_aws_creds_with_env(self, client_OsClientfactory):
        # verify config parsing fails when configuration source for aws creds is set to None
        self.config_opts_parsing_aws_creds("None")

        # verify config parsing fails when configuration for aws creds is missing a required config key
        override_config = {
            "OSB_DATASTORE_AWS_ACCESS_KEY_ID": None
        }
        self.config_opts_parsing_aws_creds("environment", override_datastore=override_config)



        # validate client_options when session_token is passed
        enable_role_access = [False, True]
        for role_based in enable_role_access:
            # verify config parsing is successful when all required parameters are present
            config_opts = self.config_opts_parsing_aws_creds("environment", role_based=role_based)

            expected_client_options = {
                "use_ssl": True,
                "timeout": 120,
                "amazon_aws_log_in": 'client_option',
                "aws_access_key_id": config_opts["_datastore_aws_access_key_id"],
                "aws_secret_access_key": config_opts["_datastore_aws_secret_access_key"],
                "service": config_opts["_datastore_aws_service"],
                "region": config_opts["_datastore_aws_region"],
                "verify_certs": config_opts["_datastore_verify_certs"]
            }

            if role_based:
                expected_client_options["aws_session_token"] = config_opts["_datastore_aws_session_token"]

            client_OsClientfactory.assert_called_with(
                hosts=[{"host": config_opts["_datastore_host"], "port": config_opts["_datastore_port"]}],
                client_options=expected_client_options
            )


    def config_opts_parsing(self, password_configuration):
        cfg = config.Config()

        _datastore_host = ".".join([str(random.randint(1, 254)) for _ in range(4)])
        _datastore_port = random.randint(1024, 65535)
        _datastore_secure = random.choice(["True", "true"])
        _datastore_user = "".join([random.choice(string.ascii_letters) for _ in range(8)])
        _datastore_password = "".join([random.choice(string.ascii_letters + string.digits + "_-@#$/") for _ in range(12)])
        _datastore_verify_certs = random.choice([True, False])

        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.user", _datastore_user)

        if password_configuration == "config":
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.password", _datastore_password)
        elif password_configuration == "environment":
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setenv("OSB_DATASTORE_PASSWORD", _datastore_password)

        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.ssl.verification_mode", "none")

        try:
            metrics.OsClientFactory(cfg)
        except exceptions.ConfigError as e:
            if password_configuration is not None:
                raise

            assert (
                e.message
                == "No password configured through [results_publishing] configuration or OSB_DATASTORE_PASSWORD environment variable."
            )
            return

        return {
            "_datastore_user": _datastore_user,
            "_datastore_host": _datastore_host,
            "_datastore_password": _datastore_password,
            "_datastore_port": _datastore_port,
            "_datastore_verify_certs": _datastore_verify_certs
        }

    def config_opts_parsing_aws_creds(self, configuration_source, override_datastore=None, role_based=False):
        if override_datastore is None:
            override_datastore = {}
        cfg = config.Config()

        _datastore_host = ".".join([str(random.randint(1, 254)) for _ in range(4)])
        _datastore_port = random.randint(1024, 65535)
        _datastore_secure = random.choice(["True", "true"])
        _datastore_user = ""
        _datastore_password = ""
        _datastore_verify_certs = random.choice([True, False])
        _datastore_amazon_aws_log_in = configuration_source
        _datastore_aws_access_key_id = "".join([random.choice(string.digits) for _ in range(AWS_ACCESS_KEY_ID_LENGTH)])
        _datastore_aws_secret_access_key = "".join([random.choice(string.ascii_letters + string.digits) \
                                                    for _ in range(AWS_SECRET_ACCESS_KEY_LENGTH)])
        _datastore_aws_service = random.choice(['es', 'aoss'])
        _datastore_aws_region = random.choice(['us-east-1', 'eu-west-1'])

        # optional
        _datastore_aws_session_token = "".join([random.choice(string.ascii_letters + string.digits) \
                                                for _ in range(AWS_SESSION_TOKEN_LENGTH)])

        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.host", _datastore_host)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.port", _datastore_port)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.secure", _datastore_secure)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.user", _datastore_user)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.password", _datastore_password)
        cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.amazon_aws_log_in",
                _datastore_amazon_aws_log_in)

        if _datastore_amazon_aws_log_in == 'config':
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.aws_access_key_id",
                    _datastore_aws_access_key_id)
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.aws_secret_access_key",
                    _datastore_aws_secret_access_key)
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.service", _datastore_aws_service)
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.region", _datastore_aws_region)
            if role_based:
                cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.aws_session_token", _datastore_aws_session_token)
        elif _datastore_amazon_aws_log_in == 'environment':
            monkeypatch = pytest.MonkeyPatch()
            monkeypatch.setenv("OSB_DATASTORE_AWS_ACCESS_KEY_ID", _datastore_aws_access_key_id)
            monkeypatch.setenv("OSB_DATASTORE_AWS_SECRET_ACCESS_KEY", _datastore_aws_secret_access_key)
            monkeypatch.setenv("OSB_DATASTORE_SERVICE", _datastore_aws_service)
            monkeypatch.setenv("OSB_DATASTORE_REGION", _datastore_aws_region)
            if role_based:
                monkeypatch.setenv("OSB_DATASTORE_AWS_SESSION_TOKEN", _datastore_aws_session_token)


        if not _datastore_verify_certs:
            cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.ssl.verification_mode", "none")

        if override_datastore:
            if _datastore_amazon_aws_log_in == 'config':
                for k, v in override_datastore.items():
                    cfg.add(config.Scope.applicationOverride, "results_publishing", k, v)
            elif _datastore_amazon_aws_log_in == 'environment':
                monkeypatch = pytest.MonkeyPatch()
                for k, v in override_datastore.items():
                    if v is not None:
                        monkeypatch.setenv(k, v)
                    else:
                        monkeypatch.delenv(k)
        try:
            metrics.OsClientFactory(cfg)
        except exceptions.ConfigError as e:
            if configuration_source == "config":
                missing_aws_credentials_message = "Missing AWS credentials through datastore.aws_access_key_id, " \
                                                  "datastore.aws_secret_access_key, datastore.region, " \
                                                  "datastore.service in the config file."
            elif configuration_source == "environment":
                missing_aws_credentials_message = "Missing AWS credentials through " \
                                                  "OSB_DATASTORE_AWS_ACCESS_KEY_ID, " \
                                                  "OSB_DATASTORE_AWS_SECRET_ACCESS_KEY, " \
                                                  "OSB_DATASTORE_REGION, OSB_DATASTORE_SERVICE " \
                                                  "environment variables."

            else:
                missing_aws_credentials_message = "datastore.amazon_aws_log_in can only be one of " \
                                                  "'environment' or 'config'"
            assert e.message == missing_aws_credentials_message
            return

        response = {
            "_datastore_user": _datastore_user,
            "_datastore_host": _datastore_host,
            "_datastore_password": _datastore_password,
            "_datastore_port": _datastore_port,
            "_datastore_verify_certs": _datastore_verify_certs,
            "_datastore_aws_access_key_id": _datastore_aws_access_key_id,
            "_datastore_aws_secret_access_key": _datastore_aws_secret_access_key,
            "_datastore_aws_service": _datastore_aws_service,
            "_datastore_aws_region": _datastore_aws_region
        }

        if role_based:
            response["_datastore_aws_session_token"] = _datastore_aws_session_token

        return response

    def test_raises_sytem_setup_error_on_connection_problems(self):
        def raise_connection_error():
            raise opensearchpy.exceptions.ConnectionError("unit-test")

        client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9200"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_connection_error)
        self.assertEqual("Could not connect to your OpenSearch metrics store. Please check that it is running on host [127.0.0.1] at "
                         "port [9200] or fix the configuration in [%s/benchmark.ini]." % paths.benchmark_confdir(),
                         ctx.exception.args[0])

    def test_raises_sytem_setup_error_on_authentication_problems(self):
        def raise_authentication_error():
            raise opensearchpy.exceptions.AuthenticationException("unit-test")

        client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authentication_error)
        self.assertEqual("The configured user could not authenticate against your OpenSearch metrics store running on host [127.0.0.1] "
                         "at port [9243] (wrong password?). Please fix the configuration in [%s/benchmark.ini]."
                         % paths.benchmark_confdir(), ctx.exception.args[0])

    def test_raises_sytem_setup_error_on_authorization_problems(self):
        def raise_authorization_error():
            raise opensearchpy.exceptions.AuthorizationException("unit-test")

        client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.SystemSetupError) as ctx:
            client.guarded(raise_authorization_error)
        self.assertEqual("The configured user does not have enough privileges to run the operation [raise_authorization_error] against "
                         "your OpenSearch metrics store running on host [127.0.0.1] at port [9243]. Please "
                         "specify a user with enough privileges in the configuration in [%s/benchmark.ini]."
                         % paths.benchmark_confdir(), ctx.exception.args[0])

    def test_raises_benchmark_error_on_unknown_problems(self):
        def raise_unknown_error():
            raise opensearchpy.exceptions.SerializationError("unit-test")

        client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            client.guarded(raise_unknown_error)
        self.assertEqual("An unknown error occurred while running the operation [raise_unknown_error] against your OpenSearch metrics "
                         "store on host [127.0.0.1] at port [9243].", ctx.exception.args[0])

    def test_retries_on_various_transport_errors(self):
        @mock.patch("random.random")
        @mock.patch("osbenchmark.time.sleep")
        def test_transport_error_retries(side_effect, expected_logging_calls, expected_sleep_calls, mocked_sleep, mocked_random):
            # should return on first success
            operation = mock.Mock(side_effect=side_effect)

            # Disable additional randomization time in exponential backoff calls
            mocked_random.return_value = 0

            client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))

            logger = logging.getLogger("osbenchmark.metrics")
            with mock.patch.object(logger, "debug") as mocked_debug_logger:
                test_result = client.guarded(operation)
                mocked_sleep.assert_has_calls(expected_sleep_calls)
                mocked_debug_logger.assert_has_calls(
                    expected_logging_calls,
                    any_order=True
                )
                self.assertEqual("success", test_result)

        max_retry = 10
        all_err_codes = TransportErrors.err_return_codes
        transport_errors = TransportErrors(max_err_responses=max_retry)
        rnd_err_codes = transport_errors.code_list
        rnd_side_effects = transport_errors.side_effects
        rnd_mocked_logger_calls = []

        # The sec to sleep for 10 transport errors is
        # [1, 2, 4, 8, 16, 32, 64, 128, 256, 512] ~> 17.05min in total
        sleep_slots = [float(2 ** i) for i in range(0, max_retry)]
        mocked_sleep_calls = [mock.call(sleep_slots[i]) for i in range(0, max_retry)]

        for rnd_err_idx, rnd_err_code in enumerate(rnd_err_codes):
            # List of logger.debug calls to expect
            rnd_mocked_logger_calls.append(
                mock.call("%s (code: %d) in attempt [%d/%d]. Sleeping for [%f] seconds.",
                          all_err_codes[rnd_err_code], rnd_err_code,
                          rnd_err_idx + 1, max_retry + 1, sleep_slots[rnd_err_idx])
            )
        # pylint: disable=no-value-for-parameter
        test_transport_error_retries(rnd_side_effects,
                                     rnd_mocked_logger_calls,
                                     mocked_sleep_calls)

    @mock.patch("osbenchmark.time.sleep")
    def test_fails_after_too_many_errors(self, mocked_sleep):
        def random_transport_error(rnd_resp_code):
            raise opensearchpy.exceptions.TransportError(rnd_resp_code, TransportErrors.err_return_codes[rnd_resp_code])

        client = metrics.OsClient(OsClientTests.ClientMock([{"host": "127.0.0.1", "port": "9243"}]))
        rnd_code = random.choice(list(TransportErrors.err_return_codes))

        with self.assertRaises(exceptions.BenchmarkError) as ctx:
            client.guarded(random_transport_error, rnd_code)

        self.assertEqual("A transport error occurred while running the operation "
                         "[random_transport_error] against your OpenSearch metrics "
                         "store on host [127.0.0.1] at port [9243].",
                         ctx.exception.args[0])


class OsMetricsTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "workload", "params", {"shard-count": 3})
        self.metrics_store = metrics.OsMetricsStore(self.cfg,
                                                    client_factory_class=MockClientFactory,
                                                    index_template_provider_class=DummyIndexTemplateProvider,
                                                    clock=StaticClock)
        # get hold of the mocked client...
        self.es_mock = self.metrics_store._client
        self.es_mock.exists.return_value = False

    def test_put_value_without_meta_info(self):
        throughput = 5000
        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append", "defaults", create=True)

        self.metrics_store.put_value_cluster_level("indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "relative-time-ms": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "workload": "test",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "append",
            "provision-config-instance": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="benchmark-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_value_with_explicit_timestamps(self):
        throughput = 5000
        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append", "defaults", create=True)

        self.metrics_store.put_value_cluster_level(name="indexing_throughput", value=throughput, unit="docs/s",
                                                   absolute_time=0, relative_time=10)
        expected_doc = {
            "@timestamp": 0,
            "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "relative-time-ms": 10000,
            "environment": "unittest",
            "sample-type": "normal",
            "workload": "test",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "append",
            "provision-config-instance": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {}
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="benchmark-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_value_with_meta_info(self):
        throughput = 5000
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "test_execution", "user.tag", "intention:testing,disk_type:hdd")
        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_value_node_level("node0", "indexing_throughput", throughput, "docs/s")
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "relative-time-ms": 0,
            "environment": "unittest",
            "sample-type": "normal",
            "workload": "test",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "append",
            "provision-config-instance": "defaults",
            "name": "indexing_throughput",
            "value": throughput,
            "unit": "docs/s",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0"
            }
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="benchmark-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_doc_no_meta_data(self):
        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append", "defaults", create=True)

        self.metrics_store.put_doc(doc={
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        })
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "relative-time-ms": 0,
            "environment": "unittest",
            "workload": "test",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "append",
            "provision-config-instance": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="benchmark-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_put_doc_with_metadata(self):
        # add a user-defined tag
        self.cfg.add(config.Scope.application, "test_execution", "user.tag", "intention:testing,disk_type:hdd")
        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append", "defaults", create=True)

        # Ensure we also merge in cluster level meta info
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "source_revision", "abc123")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_name", "Darwin")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node0", "os_version", "15.4.0")
        # Ensure we separate node level info by node
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_name", "Linux")
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.node, "node1", "os_version", "4.2.0-18-generic")

        self.metrics_store.put_doc(doc={
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte"
        }, level=metrics.MetaInfoScope.node,
            node_name="node0",
            meta_data={
                "node_type": "hot"
            })
        expected_doc = {
            "@timestamp": StaticClock.NOW * 1000,
            "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "relative-time-ms": 0,
            "environment": "unittest",
            "workload": "test",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "append",
            "provision-config-instance": "defaults",
            "name": "custom_metric",
            "total": 1234567,
            "per-shard": [17, 18, 1289, 273, 222],
            "unit": "byte",
            "meta": {
                "tag_intention": "testing",
                "tag_disk_type": "hdd",
                "source_revision": "abc123",
                "os_name": "Darwin",
                "os_version": "15.4.0",
                "node_type": "hot"
            }
        }
        self.metrics_store.close()
        self.es_mock.exists.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.create_index.assert_called_with(index="benchmark-metrics-2016-01")
        self.es_mock.bulk_index.assert_called_with(index="benchmark-metrics-2016-01", doc_type="_doc", items=[expected_doc])

    def test_get_one(self):
        duration = StaticClock.NOW * 1000
        search_result = {
            "hits": {
                "total": 2,
                "hits": [
                    {
                        "_source": {
                            "relative-time-ms": duration,
                            "value": 500
                        }
                    },
                    {
                        "_source": {
                            "relative-time-ms": duration-200,
                            "value": 700
                        }
                    },
                ]
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "service_time"
                            }
                        },
                        {
                            "term": {
                                "task": "task1"
                            }
                        }
                    ]
                }
            },
            "size": 1,
            "sort": [
                {"relative-time-ms": {"order": "desc" }}
                ]
        }

        actual_duration = self.metrics_store.get_one("service_time", task="task1", mapper=lambda doc: doc["relative-time-ms"],
                                                         sort_key="relative-time-ms", sort_reverse=True)

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(duration, actual_duration)

    def test_get_one_no_hits(self):
        duration = None
        search_result = {
            "hits": {
                "total": 0,
                "hits": []
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "latency"
                            }
                        },
                        {
                            "term": {
                                "task": "task2"
                            }
                        }
                    ]
                }
            },
            "size": 1,
            "sort": [
                {"value": {"order": "asc" }}
                ]
        }

        actual_duration = self.metrics_store.get_one("latency", task="task2", mapper=lambda doc: doc["value"],
                                                         sort_key="value", sort_reverse=False)

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(duration, actual_duration)

    def test_get_value(self):
        throughput = 5000
        search_result = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": StaticClock.NOW * 1000,
                            "value": throughput
                        }
                    }
                ]
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        }
                    ]
                }
            },
            "size": 1
        }

        actual_throughput = self.metrics_store.get_one("indexing_throughput")

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(throughput, actual_throughput)

    def test_get_per_node_value(self):
        index_size = 5000
        search_result = {
            "hits": {
                "total": 1,
                "hits": [
                    {
                        "_source": {
                            "@timestamp": StaticClock.NOW * 1000,
                            "value": index_size
                        }
                    }
                ]
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "final_index_size_bytes"
                            }
                        },
                        {
                            "term": {
                                "meta.node_name": "benchmark-node-3"
                            }
                        }
                    ]
                }
            },
            "size": 1
        }

        actual_index_size = self.metrics_store.get_one("final_index_size_bytes", node_name="benchmark-node-3")

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(index_size, actual_index_size)

    def test_get_mean(self):
        mean_throughput = 1734
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "metric_stats": {
                    "count": 17,
                    "min": 1208,
                    "max": 1839,
                    "avg": mean_throughput,
                    "sum": 28934
                }
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        },
                        {
                            "term": {
                                "operation-type": "bulk"
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "metric_stats": {
                    "stats": {
                        "field": "value"
                    }
                }
            }
        }

        actual_mean_throughput = self.metrics_store.get_mean("indexing_throughput", operation_type="bulk")

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(mean_throughput, actual_mean_throughput)

    def test_get_median(self):
        median_throughput = 30535
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "percentile_stats": {
                    "values": {
                        "50.0": median_throughput
                    }
                }
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "indexing_throughput"
                            }
                        },
                        {
                            "term": {
                                "operation-type": "bulk"
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "percentile_stats": {
                    "percentiles": {
                        "field": "value",
                        "percents": ["50.0"]
                    }
                }
            }
        }

        actual_median_throughput = self.metrics_store.get_median("indexing_throughput", operation_type="bulk")

        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)

        self.assertEqual(median_throughput, actual_median_throughput)

    def test_get_error_rate_implicit_zero(self):
        self.assertEqual(0.0, self._get_error_rate(buckets=[
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 0

            }
        ]))

    def test_get_error_rate_explicit_zero(self):
        self.assertEqual(0.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 0
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 500
            }
        ]))

    def test_get_error_rate_implicit_one(self):
        self.assertEqual(1.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 123
            }
        ]))

    def test_get_error_rate_explicit_one(self):
        self.assertEqual(1.0, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 123
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 0
            }
        ]))

    def test_get_error_rate_mixed(self):
        self.assertEqual(0.5, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 500
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 500
            }
        ]))

    def test_get_error_rate_additional_unknown_key(self):
        self.assertEqual(0.25, self._get_error_rate(buckets=[
            {
                "key": 0,
                "key_as_string": "false",
                "doc_count": 500
            },
            {
                "key": 1,
                "key_as_string": "true",
                "doc_count": 1500
            },
            {
                "key": 2,
                "key_as_string": "undefined_for_test",
                "doc_count": 13700
            }
        ]))

    def _get_error_rate(self, buckets):
        search_result = {
            "hits": {
                "total": 1,
            },
            "aggregations": {
                "error_rate": {
                    "buckets": buckets
                }
            }
        }
        self.es_mock.search = mock.MagicMock(return_value=search_result)

        self.metrics_store.open(
            OsMetricsTests.TEST_EXECUTION_ID,
            OsMetricsTests.TEST_EXECUTION_TIMESTAMP,
            "test", "append-no-conflicts", "defaults")

        expected_query = {
            "query": {
                "bool": {
                    "filter": [
                        {
                            "term": {
                                "test-execution-id": OsMetricsTests.TEST_EXECUTION_ID
                            }
                        },
                        {
                            "term": {
                                "name": "service_time"
                            }
                        },
                        {
                            "term": {
                                "task": "scroll_query"
                            }
                        }
                    ]
                }
            },
            "size": 0,
            "aggs": {
                "error_rate": {
                    "terms": {
                        "field": "meta.success"
                    }
                }
            }
        }

        actual_error_rate = self.metrics_store.get_error_rate("scroll_query")
        self.es_mock.search.assert_called_with(index="benchmark-metrics-2016-01", body=expected_query)
        return actual_error_rate


class OsTestExecutionStoreTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "time.start", OsTestExecutionStoreTests.TEST_EXECUTION_TIMESTAMP)
        self.cfg.add(config.Scope.application, "system", "test_execution.id", FileTestExecutionStoreTests.TEST_EXECUTION_ID)
        self.test_execution_store = metrics.EsTestExecutionStore(self.cfg,
                                              client_factory_class=MockClientFactory,
                                              index_template_provider_class=DummyIndexTemplateProvider,
                                              )
        # get hold of the mocked client...
        self.es_mock = self.test_execution_store.client

    def test_find_existing_test_execution_by_test_execution_id(self):
        self.es_mock.search.return_value = {
            "hits": {
                "total": {
                    "value": 1,
                    "relation": "eq"
                },
                "hits": [
                    {
                        "_source": {
                            "benchmark-version": "0.4.4",
                            "environment": "unittest",
                            "test-execution-id": OsTestExecutionStoreTests.TEST_EXECUTION_ID,
                            "test-execution-timestamp": "20160131T000000Z",
                            "pipeline": "from-sources",
                            "workload": "unittest",
                            "test_procedure": "index",
                            "workload-revision": "abc1",
                            "provision-config-instance": "defaults",
                            "results": {
                                "young_gc_time": 100,
                                "old_gc_time": 5,
                            }
                        }
                    }
                ]
            }
        }

        test_execution = self.test_execution_store.find_by_test_execution_id(test_execution_id=OsTestExecutionStoreTests.TEST_EXECUTION_ID)
        self.assertEqual(test_execution.test_execution_id, OsTestExecutionStoreTests.TEST_EXECUTION_ID)

    def test_does_not_find_missing_test_execution_by_test_execution_id(self):
        self.es_mock.search.return_value = {
            "hits": {
                "total": {
                    "value": 0,
                    "relation": "eq"
                },
                "hits": []
            }
        }

        with self.assertRaisesRegex(exceptions.NotFound, r"No test_execution with test_execution id \[.*\]"):
            self.test_execution_store.find_by_test_execution_id(test_execution_id="some invalid test_execution id")

    def test_store_test_execution(self):
        schedule = [
            workload.Task("index #1", workload.Operation("index", workload.OperationType.Bulk))
        ]

        t = workload.Workload(name="unittest",
                        indices=[workload.Index(name="tests", types=["_doc"])],
                        test_procedures=[workload.Procedure(name="index", default=True, schedule=schedule)])

        test_execution = metrics.TestExecution(benchmark_version="0.4.4", benchmark_revision="123abc", environment_name="unittest",
                            test_execution_id=OsTestExecutionStoreTests.TEST_EXECUTION_ID,
                            test_execution_timestamp=OsTestExecutionStoreTests.TEST_EXECUTION_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, workload=t, workload_params={"shard-count": 3},
                            test_procedure=t.default_test_procedure,
                            provision_config_instance="defaults",
                            provision_config_instance_params={"heap_size": "512mb"},
                            plugin_params=None,
                            workload_revision="abc1", provision_config_revision="abc12333", distribution_version="5.0.0",
                            distribution_flavor="default", revision="aaaeeef",
                            results=OsTestExecutionStoreTests.DictHolder(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "task": "index #1",
                                            "operation": "index",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.test_execution_store.store_test_execution(test_execution)

        expected_doc = {
            "benchmark-version": "0.4.4",
            "benchmark-revision": "123abc",
            "environment": "unittest",
            "test-execution-id": OsTestExecutionStoreTests.TEST_EXECUTION_ID,
            "test-execution-timestamp": "20160131T000000Z",
            "pipeline": "from-sources",
            "user-tags": {
                "os": "Linux"
            },
            "workload": "unittest",
            "workload-params": {
                "shard-count": 3
            },
            "test_procedure": "index",
            "workload-revision": "abc1",
            "provision-config-instance": "defaults",
            "provision-config-instance-params": {
                "heap_size": "512mb"
            },
            "cluster": {
                "revision": "aaaeeef",
                "distribution-version": "5.0.0",
                "distribution-flavor": "default",
                "provision-config-revision": "abc12333",
            },
            "results": {
                "young_gc_time": 100,
                "old_gc_time": 5,
                "op_metrics": [
                    {
                        "task": "index #1",
                        "operation": "index",
                        "throughput": {
                            "min": 1000,
                            "median": 1250,
                            "max": 1500,
                            "unit": "docs/s"
                        }
                    }
                ]
            }
        }
        self.es_mock.index.assert_called_with(index="benchmark-test-executions-2016-01",
                                              doc_type="_doc",
                                              id=OsTestExecutionStoreTests.TEST_EXECUTION_ID,
                                              item=expected_doc)


class OsResultsStoreTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "system", "time.start", OsTestExecutionStoreTests.TEST_EXECUTION_TIMESTAMP)
        self.results_store = metrics.OsResultsStore(self.cfg,
                                                    client_factory_class=MockClientFactory,
                                                    index_template_provider_class=DummyIndexTemplateProvider,
                                                    )
        # get hold of the mocked client...
        self.es_mock = self.results_store.client

    def test_store_results(self):
        schedule = [
            workload.Task("index #1", workload.Operation("index", workload.OperationType.Bulk))
        ]

        t = workload.Workload(name="unittest-workload",
                        indices=[workload.Index(name="tests", types=["_doc"])],
                        test_procedures=[workload.Procedure(
                            name="index", default=True, meta_data={"saturation": "70% saturated"}, schedule=schedule)],
                        meta_data={"workload-type": "saturation-degree", "saturation": "oversaturation"})

        test_execution = metrics.TestExecution(benchmark_version="0.4.4", benchmark_revision="123abc", environment_name="unittest",
                            test_execution_id=OsResultsStoreTests.TEST_EXECUTION_ID,
                            test_execution_timestamp=OsResultsStoreTests.TEST_EXECUTION_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, workload=t, workload_params=None,
                            test_procedure=t.default_test_procedure,
                            provision_config_instance="4gheap",
                            provision_config_instance_params=None,
                            plugin_params={"some-param": True},
                            workload_revision="abc1", provision_config_revision="123ab", distribution_version="5.0.0",
                            distribution_flavor="oss", results=metrics.GlobalStats(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "task": "index #1",
                                            "operation": "index",
                                            # custom op-metric which will override the defaults provided by the test_execution
                                            "meta": {
                                                "workload-type": "saturation-degree",
                                                "saturation": "70% saturated",
                                                "op-type": "bulk"
                                            },
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.results_store.store_results(test_execution)

        expected_docs = [
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": "123abc",
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "active": True,
                "name": "old_gc_time",
                "value": {
                    "single": 5
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated"
                }
            },
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": "123abc",
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "active": True,
                "name": "throughput",
                "task": "index #1",
                "operation": "index",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s"
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated",
                    "op-type": "bulk"
                }
            },
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": "123abc",
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": "oss",
                "distribution-version": "5.0.0",
                "distribution-major-version": 5,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "plugin-params": {
                    "some-param": True
                },
                "active": True,
                "name": "young_gc_time",
                "value": {
                    "single": 100
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated"
                }
            }
        ]
        self.es_mock.bulk_index.assert_called_with(
            index="benchmark-results-2016-01",
            doc_type="_doc",
            items=expected_docs)

    def test_store_results_with_missing_version(self):
        schedule = [
            workload.Task("index #1", workload.Operation("index", workload.OperationType.Bulk))
        ]

        t = workload.Workload(name="unittest-workload",
                        indices=[workload.Index(name="tests", types=["_doc"])],
                        test_procedures=[workload.Procedure(
                            name="index", default=True, meta_data={"saturation": "70% saturated"}, schedule=schedule)],
                        meta_data={"workload-type": "saturation-degree", "saturation": "oversaturation"})

        test_execution = metrics.TestExecution(benchmark_version="0.4.4", benchmark_revision=None, environment_name="unittest",
                            test_execution_id=OsResultsStoreTests.TEST_EXECUTION_ID,
                            test_execution_timestamp=OsResultsStoreTests.TEST_EXECUTION_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, workload=t, workload_params=None,
                            test_procedure=t.default_test_procedure,
                            provision_config_instance="4gheap",
                            provision_config_instance_params=None,
                            plugin_params=None,
                            workload_revision="abc1",
                            provision_config_revision="123ab",
                            distribution_version=None,
                            distribution_flavor=None, results=metrics.GlobalStats(
                {
                    "young_gc_time": 100,
                    "old_gc_time": 5,
                    "op_metrics": [
                        {
                            "task": "index #1",
                            "operation": "index",
                            # custom op-metric which will override the defaults provided by the test_execution
                            "meta": {
                                "workload-type": "saturation-degree",
                                "saturation": "70% saturated",
                                "op-type": "bulk"
                            },
                            "throughput": {
                                "min": 1000,
                                "median": 1250,
                                "max": 1500,
                                "unit": "docs/s"
                            }
                        }
                    ]
                })
                            )

        self.results_store.store_results(test_execution)

        expected_docs = [
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": None,
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "active": True,
                "name": "old_gc_time",
                "value": {
                    "single": 5
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated"
                }
            },
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": None,
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "active": True,
                "name": "throughput",
                "task": "index #1",
                "operation": "index",
                "value": {
                    "min": 1000,
                    "median": 1250,
                    "max": 1500,
                    "unit": "docs/s"
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated",
                    "op-type": "bulk"
                }
            },
            {
                "benchmark-version": "0.4.4",
                "benchmark-revision": None,
                "environment": "unittest",
                "test-execution-id": OsResultsStoreTests.TEST_EXECUTION_ID,
                "test-execution-timestamp": "20160131T000000Z",
                "distribution-flavor": None,
                "distribution-version": None,
                "user-tags": {
                    "os": "Linux"
                },
                "workload": "unittest-workload",
                "provision-config-revision": "123ab",
                "workload-revision": "abc1",
                "test_procedure": "index",
                "provision-config-instance": "4gheap",
                "active": True,
                "name": "young_gc_time",
                "value": {
                    "single": 100
                },
                "meta": {
                    "workload-type": "saturation-degree",
                    "saturation": "70% saturated"
                }
            }
        ]
        self.es_mock.bulk_index.assert_called_with(index="benchmark-results-2016-01", doc_type="_doc", items=expected_docs)


class InMemoryMetricsStoreTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "workload", "params", {})
        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)

    def tearDown(self):
        del self.metrics_store
        del self.cfg

    def test_get_one(self):
        duration = StaticClock.NOW
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("service_time", 500, "ms", relative_time=duration-400, task="task1")
        self.metrics_store.put_value_cluster_level("service_time", 600, "ms", relative_time=duration, task="task1")
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", relative_time=duration-300)

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        actual_duration = self.metrics_store.get_one("service_time", task="task1", mapper=lambda doc: doc["relative-time-ms"],
                                                         sort_key="relative-time-ms", sort_reverse=True)

        self.assertEqual(duration * 1000, actual_duration)

    def test_get_one_no_hits(self):
        duration = StaticClock.NOW
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", relative_time=duration-300)

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        actual_duration = self.metrics_store.get_one("service_time", task="task1", mapper=lambda doc: doc["relative-time-ms"],
                                                     sort_key="relative-time-ms", sort_reverse=True)

        self.assertIsNone(actual_duration)

    def test_get_value(self):
        throughput = 5000
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("indexing_throughput", 1, "docs/s", sample_type=metrics.SampleType.Warmup)
        self.metrics_store.put_value_cluster_level("indexing_throughput", throughput, "docs/s")
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(1, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(throughput, self.metrics_store.get_one("indexing_throughput", sample_type=metrics.SampleType.Normal))

    def test_get_percentile(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assert_equal_percentiles("query_latency", [100.0], {100.0: 1000.0})
        self.assert_equal_percentiles("query_latency", [99.0], {99.0: 990.0})
        self.assert_equal_percentiles("query_latency", [99.9], {99.9: 999.0})
        self.assert_equal_percentiles("query_latency", [0.0], {0.0: 1.0})

        self.assert_equal_percentiles("query_latency", [99, 99.9, 100], {99: 990.0, 99.9: 999.0, 100: 1000.0})

    def test_get_mean(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        for i in range(1, 100):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertAlmostEqual(50, self.metrics_store.get_mean("query_latency"))

    def test_get_median(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        for i in range(1, 1001):
            self.metrics_store.put_value_cluster_level("query_latency", float(i), "ms")

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertAlmostEqual(500.5, self.metrics_store.get_median("query_latency"))

    def assert_equal_percentiles(self, name, percentiles, expected_percentiles):
        actual_percentiles = self.metrics_store.get_percentiles(name, percentiles=percentiles)
        self.assertEqual(len(expected_percentiles), len(actual_percentiles))
        for percentile, actual_percentile_value in actual_percentiles.items():
            self.assertAlmostEqual(expected_percentiles[percentile], actual_percentile_value, places=1,
                                   msg=str(percentile) + "th percentile differs")

    def test_externalize_and_bulk_add(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB")

        self.assertEqual(1, len(self.metrics_store.docs))
        memento = self.metrics_store.to_externalizable()

        self.metrics_store.close()
        del self.metrics_store

        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)
        self.assertEqual(0, len(self.metrics_store.docs))

        self.metrics_store.bulk_add(memento)
        self.assertEqual(1, len(self.metrics_store.docs))
        self.assertEqual(1000, self.metrics_store.get_one("final_index_size"))

    def test_meta_data_per_document(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.add_meta_info(metrics.MetaInfoScope.cluster, None, "cluster-name", "test")

        self.metrics_store.put_value_cluster_level("final_index_size", 1000, "GB", meta_data={
            "fs-block-size-bytes": 512
        })
        self.metrics_store.put_value_cluster_level("final_bytes_written", 1, "TB", meta_data={
            "io-batch-size-kb": 4
        })

        self.assertEqual(2, len(self.metrics_store.docs))
        self.assertEqual({
            "cluster-name": "test",
            "fs-block-size-bytes": 512
        }, self.metrics_store.docs[0]["meta"])

        self.assertEqual({
            "cluster-name": "test",
            "io-batch-size-kb": 4
        }, self.metrics_store.docs[1]["meta"])

    def test_get_error_rate_zero_without_samples(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_by_sample_type(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Warmup,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(1.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))

    def test_get_error_rate_mixed(self):
        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults", create=True)
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": False})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})
        self.metrics_store.put_value_cluster_level("service_time", 3.0, "ms", task="term-query", sample_type=metrics.SampleType.Normal,
                                                   meta_data={"success": True})

        self.metrics_store.close()

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-no-conflicts", "defaults")

        self.assertEqual(0.0, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Warmup))
        self.assertEqual(0.2, self.metrics_store.get_error_rate("term-query", sample_type=metrics.SampleType.Normal))


class FileTestExecutionStoreTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "6ebc6e53-ee20-4b0c-99b4-09697987e9f4"

    class DictHolder:
        def __init__(self, d):
            self.d = d

        def as_dict(self):
            return self.d

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.test_executions.max_results", 100)
        self.cfg.add(config.Scope.application, "system", "time.start", FileTestExecutionStoreTests.TEST_EXECUTION_TIMESTAMP)
        self.cfg.add(
            config.Scope.application, "system", "test_execution.id",
            FileTestExecutionStoreTests.TEST_EXECUTION_ID)
        self.test_execution_store = metrics.FileTestExecutionStore(self.cfg)

    def test_test_execution_not_found(self):
        with self.assertRaisesRegex(exceptions.NotFound, r"No test execution with test execution id \[.*\]"):
            # did not store anything yet
            self.test_execution_store.find_by_test_execution_id(FileTestExecutionStoreTests.TEST_EXECUTION_ID)

    def test_store_test_execution(self):
        schedule = [
            workload.Task("index #1", workload.Operation("index", workload.OperationType.Bulk))
        ]

        t = workload.Workload(name="unittest",
                        indices=[workload.Index(name="tests", types=["_doc"])],
                        test_procedures=[workload.Procedure(name="index", default=True, schedule=schedule)])

        test_execution = metrics.TestExecution(
            benchmark_version="0.4.4", benchmark_revision="123abc", environment_name="unittest",
                            test_execution_id=FileTestExecutionStoreTests.TEST_EXECUTION_ID,
                            test_execution_timestamp=FileTestExecutionStoreTests.TEST_EXECUTION_TIMESTAMP,
                            pipeline="from-sources", user_tags={"os": "Linux"}, workload=t, workload_params={"clients": 12},
                            test_procedure=t.default_test_procedure,
                            provision_config_instance="4gheap",
                            provision_config_instance_params=None,
                            plugin_params=None,
                            workload_revision="abc1",
                            provision_config_revision="abc12333",
                            distribution_version="5.0.0",
                            distribution_flavor="default", revision="aaaeeef",
                            results=FileTestExecutionStoreTests.DictHolder(
                                {
                                    "young_gc_time": 100,
                                    "old_gc_time": 5,
                                    "op_metrics": [
                                        {
                                            "task": "index #1",
                                            "operation": "index",
                                            "throughput": {
                                                "min": 1000,
                                                "median": 1250,
                                                "max": 1500,
                                                "unit": "docs/s"
                                            }
                                        }
                                    ]
                                })
                            )

        self.test_execution_store.store_test_execution(test_execution)

        retrieved_test_execution = self.test_execution_store.find_by_test_execution_id(
            test_execution_id=FileTestExecutionStoreTests.TEST_EXECUTION_ID)
        self.assertEqual(test_execution.test_execution_id, retrieved_test_execution.test_execution_id)
        self.assertEqual(test_execution.test_execution_timestamp, retrieved_test_execution.test_execution_timestamp)
        self.assertEqual(1, len(self.test_execution_store.list()))


class StatsCalculatorTests(TestCase):
    def test_calculate_global_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "system", "test_execution.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "results_publishing", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "builder", "provision_config_instance.names", ["unittest_provision_config_instance"])
        cfg.add(config.Scope.application, "builder", "provision_config_instance.params", {})
        cfg.add(config.Scope.application, "builder", "plugin.params", {})
        cfg.add(config.Scope.application, "test_execution", "user.tag", "")
        cfg.add(config.Scope.application, "test_execution", "pipeline", "from-sources")
        cfg.add(config.Scope.application, "workload", "params", {})

        index1 = workload.Task(name="index #1", operation=workload.Operation(
            name="index",
            operation_type=workload.OperationType.Bulk,
            params=None))
        index2 = workload.Task(name="index #2", operation=workload.Operation(
            name="index",
            operation_type=workload.OperationType.Bulk,
            params=None))
        test_procedure = workload.Procedure(name="unittest", schedule=[index1, index2], default=True)
        t = workload.Workload("unittest", "unittest-workload", test_procedures=[test_procedure])

        store = metrics.metrics_store(cfg, read_only=False, workload=t, test_procedure=test_procedure)

        store.put_value_cluster_level("throughput", 500, unit="docs/s", task="index #1", operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 1000, unit="docs/s", task="index #1", operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("throughput", 2000, unit="docs/s", task="index #1", operation_type=workload.OperationType.Bulk)

        store.put_value_cluster_level("latency", 2800, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup)
        store.put_value_cluster_level("latency", 200, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("latency", 220, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("latency", 225, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk)

        store.put_value_cluster_level("service_time", 250, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup, meta_data={"success": False}, relative_time=536)
        store.put_value_cluster_level("service_time", 190, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk,
                                      meta_data={"success": True}, relative_time=595)
        store.put_value_cluster_level("service_time", 200, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk,
                                      meta_data={"success": False}, relative_time=709)
        store.put_value_cluster_level("service_time", 210, unit="ms", task="index #1", operation_type=workload.OperationType.Bulk,
                                      meta_data={"success": True}, relative_time=653)

        # only warmup samples
        store.put_value_cluster_level("throughput", 500, unit="docs/s", task="index #2",
                                      sample_type=metrics.SampleType.Warmup, operation_type=workload.OperationType.Bulk)
        store.put_value_cluster_level("latency", 2800, unit="ms", task="index #2", operation_type=workload.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup)
        store.put_value_cluster_level("service_time", 250, unit="ms", task="index #2", operation_type=workload.OperationType.Bulk,
                                      sample_type=metrics.SampleType.Warmup, relative_time=600)

        store.put_doc(doc={
            "name": "ml_processing_time",
            "job": "benchmark_ml_job_1",
            "min": 2.2,
            "mean": 12.3,
            "median": 17.2,
            "max": 36.0,
            "unit": "ms"
        }, level=metrics.MetaInfoScope.cluster)

        stats = metrics.calculate_results(store, metrics.create_test_execution(cfg, t, test_procedure))

        del store

        opm = stats.metrics("index #1")
        self.assertEqual(collections.OrderedDict(
            [("min", 500), ("mean", 1125), ("median", 1000), ("max", 2000), ("unit", "docs/s")]), opm["throughput"])
        self.assertEqual(collections.OrderedDict(
            [("50_0", 220), ("100_0", 225), ("mean", 215), ("unit", "ms")]), opm["latency"])
        self.assertEqual(collections.OrderedDict(
            [("50_0", 200), ("100_0", 210), ("mean", 200), ("unit", "ms")]), opm["service_time"])
        self.assertAlmostEqual(0.3333333333333333, opm["error_rate"])
        self.assertEqual(709*1000, opm["duration"])

        opm2 = stats.metrics("index #2")
        self.assertEqual(collections.OrderedDict(
            [("min", None), ("mean", None), ("median", None), ("max", None), ("unit", "docs/s")]), opm2["throughput"])

        self.assertEqual(1, len(stats.ml_processing_time))
        self.assertEqual("benchmark_ml_job_1", stats.ml_processing_time[0]["job"])
        self.assertEqual(2.2, stats.ml_processing_time[0]["min"])
        self.assertEqual(12.3, stats.ml_processing_time[0]["mean"])
        self.assertEqual(17.2, stats.ml_processing_time[0]["median"])
        self.assertEqual(36.0, stats.ml_processing_time[0]["max"])
        self.assertEqual("ms", stats.ml_processing_time[0]["unit"])
        self.assertEqual(600*1000, opm2["duration"])

    def test_calculate_system_stats(self):
        cfg = config.Config()
        cfg.add(config.Scope.application, "system", "env.name", "unittest")
        cfg.add(config.Scope.application, "system", "time.start", datetime.datetime.now())
        cfg.add(config.Scope.application, "system", "test_execution.id", "6ebc6e53-ee20-4b0c-99b4-09697987e9f4")
        cfg.add(config.Scope.application, "results_publishing", "datastore.type", "in-memory")
        cfg.add(config.Scope.application, "builder", "provision_config_instance.names", ["unittest_provision_config_instance"])
        cfg.add(config.Scope.application, "builder", "provision_config_instance.params", {})
        cfg.add(config.Scope.application, "builder", "plugin.params", {})
        cfg.add(config.Scope.application, "test_execution", "user.tag", "")
        cfg.add(config.Scope.application, "test_execution", "pipeline", "from-sources")
        cfg.add(config.Scope.application, "workload", "params", {})

        index = workload.Task(name="index #1", operation=workload.Operation(
            name="index",
            operation_type=workload.OperationType.Bulk,
            params=None))
        test_procedure = workload.Procedure(name="unittest", schedule=[index], default=True)
        t = workload.Workload("unittest", "unittest-workload", test_procedures=[test_procedure])

        store = metrics.metrics_store(cfg, read_only=False, workload=t, test_procedure=test_procedure)
        store.add_meta_info(metrics.MetaInfoScope.node, "benchmark-node-0", "node_name", "benchmark-node-0")

        store.put_value_node_level("benchmark-node-0", "final_index_size_bytes", 2048, unit="bytes")
        # ensure this value will be filtered as it does not belong to our node
        store.put_value_node_level("benchmark-node-1", "final_index_size_bytes", 4096, unit="bytes")

        stats = metrics.calculate_system_results(store, "benchmark-node-0")

        del store

        self.assertEqual([
            {
                "node": "benchmark-node-0",
                "name": "index_size",
                "value": 2048,
                "unit": "bytes"
            }
        ], stats.node_metrics)


def select(l, name, operation=None, job=None, node=None):
    for item in l:
        if item["name"] == name and item.get("operation") == operation and item.get("node") == node and item.get("job") == job:
            return item
    return None


class GlobalStatsCalculatorTests(TestCase):
    TEST_EXECUTION_TIMESTAMP = datetime.datetime(2016, 1, 31)
    TEST_EXECUTION_ID = "fb26018b-428d-4528-b36b-cf8c54a303ec"

    def setUp(self):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest")
        self.cfg.add(config.Scope.application, "workload", "params", {})
        self.metrics_store = metrics.InMemoryMetricsStore(self.cfg, clock=StaticClock)

    def tearDown(self):
        del self.metrics_store
        del self.cfg

    def test_add_administrative_task_with_error_rate_in_results(self):
        op = Operation(name='delete-index', operation_type='DeleteIndex', params={'include-in-results_publishing': False})
        task = Task('delete-index', operation=op, schedule='deterministic')
        test_procedure = Procedure(name='append-fast-with-conflicts', schedule=[task], meta_data={})

        self.metrics_store.open(InMemoryMetricsStoreTests.TEST_EXECUTION_ID, InMemoryMetricsStoreTests.TEST_EXECUTION_TIMESTAMP,
                                "test", "append-fast-with-conflicts", "defaults", create=True)
        self.metrics_store.put_doc(doc={"@timestamp": 1595896761994,
                                        "relative-time-ms": 283.382,
                                        "test-execution-id": "fb26018b-428d-4528-b36b-cf8c54a303ec",
                                        "test-execution-timestamp": "20200728T003905Z", "environment": "local",
                                        "workload": "geonames", "test_procedure": "append-fast-with-conflicts",
                                        "provision-config-instance": "defaults", "name": "service_time", "value": 72.67997100007051,
                                        "unit": "ms", "sample-type": "normal",
                                        "meta": {"source_revision": "7f634e9f44834fbc12724506cc1da681b0c3b1e3",
                                                 "distribution_version": "7.6.0", "distribution_flavor": "oss",
                                                 "success": False}, "task": "delete-index", "operation": "delete-index",
                                        "operation-type": "DeleteIndex"})

        result = GlobalStatsCalculator(store=self.metrics_store, workload=Workload(name='geonames', meta_data={}),
                                       test_procedure=test_procedure)()
        assert "delete-index" in [op_metric.get('task') for op_metric in result.op_metrics]


class GlobalStatsTests(TestCase):
    def test_as_flat_list(self):
        d = {
            "op_metrics": [
                {
                    "task": "index #1",
                    "operation": "index",
                    "throughput": {
                        "min": 450,
                        "mean": 450,
                        "median": 450,
                        "max": 452,
                        "unit": "docs/s"
                    },
                    "latency": {
                        "50": 340,
                        "100": 376,
                    },
                    "service_time": {
                        "50": 341,
                        "100": 376
                    },
                    "error_rate": 0.0,
                    "meta": {
                        "clients": 8,
                        "phase": "idx"
                    }
                },
                {
                    "task": "search #2",
                    "operation": "search",
                    "throughput": {
                        "min": 9,
                        "mean": 10,
                        "median": 10,
                        "max": 12,
                        "unit": "ops/s"
                    },
                    "latency": {
                        "50": 99,
                        "100": 111,
                    },
                    "service_time": {
                        "50": 98,
                        "100": 110
                    },
                    "error_rate": 0.1
                }
            ],
            "ml_processing_time": [
                {
                    "job": "job_1",
                    "min": 3.3,
                    "mean": 5.2,
                    "median": 5.8,
                    "max": 12.34
                },
                {
                    "job": "job_2",
                    "min": 3.55,
                    "mean": 4.2,
                    "median": 4.9,
                    "max": 9.4
                },
            ],
            "young_gc_time": 68,
            "young_gc_count": 7,
            "old_gc_time": 0,
            "old_gc_count": 0,
            "merge_time": 3702,
            "merge_time_per_shard": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms"
            },
            "merge_count": 2,
            "refresh_time": 596,
            "refresh_time_per_shard": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms"
            },
            "refresh_count": 10,
            "flush_time": None,
            "flush_time_per_shard": {},
            "flush_count": 0
        }

        s = metrics.GlobalStats(d)
        metric_list = s.as_flat_list()
        self.assertEqual({
            "name": "throughput",
            "task": "index #1",
            "operation": "index",
            "value": {
                "min": 450,
                "mean": 450,
                "median": 450,
                "max": 452,
                "unit": "docs/s"
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "throughput", operation="index"))

        self.assertEqual({
            "name": "service_time",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 341,
                "100": 376
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "service_time", operation="index"))

        self.assertEqual({
            "name": "latency",
            "task": "index #1",
            "operation": "index",
            "value": {
                "50": 340,
                "100": 376
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "latency", operation="index"))

        self.assertEqual({
            "name": "error_rate",
            "task": "index #1",
            "operation": "index",
            "value": {
                "single": 0.0
            },
            "meta": {
                "clients": 8,
                "phase": "idx"
            }
        }, select(metric_list, "error_rate", operation="index"))

        self.assertEqual({
            "name": "throughput",
            "task": "search #2",
            "operation": "search",
            "value": {
                "min": 9,
                "mean": 10,
                "median": 10,
                "max": 12,
                "unit": "ops/s"
            }
        }, select(metric_list, "throughput", operation="search"))

        self.assertEqual({
            "name": "service_time",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 98,
                "100": 110
            }
        }, select(metric_list, "service_time", operation="search"))

        self.assertEqual({
            "name": "latency",
            "task": "search #2",
            "operation": "search",
            "value": {
                "50": 99,
                "100": 111
            }
        }, select(metric_list, "latency", operation="search"))

        self.assertEqual({
            "name": "error_rate",
            "task": "search #2",
            "operation": "search",
            "value": {
                "single": 0.1
            }
        }, select(metric_list, "error_rate", operation="search"))

        self.assertEqual({
            "name": "ml_processing_time",
            "job": "job_1",
            "value": {
                "min": 3.3,
                "mean": 5.2,
                "median": 5.8,
                "max": 12.34
            }
        }, select(metric_list, "ml_processing_time", job="job_1"))

        self.assertEqual({
            "name": "ml_processing_time",
            "job": "job_2",
            "value": {
                "min": 3.55,
                "mean": 4.2,
                "median": 4.9,
                "max": 9.4
            }
        }, select(metric_list, "ml_processing_time", job="job_2"))

        self.assertEqual({
            "name": "young_gc_time",
            "value": {
                "single": 68
            }
        }, select(metric_list, "young_gc_time"))
        self.assertEqual({
            "name": "young_gc_count",
            "value": {
                "single": 7
            }
        }, select(metric_list, "young_gc_count"))

        self.assertEqual({
            "name": "old_gc_time",
            "value": {
                "single": 0
            }
        }, select(metric_list, "old_gc_time"))
        self.assertEqual({
            "name": "old_gc_count",
            "value": {
                "single": 0
            }
        }, select(metric_list, "old_gc_count"))

        self.assertEqual({
            "name": "merge_time",
            "value": {
                "single": 3702
            }
        }, select(metric_list, "merge_time"))

        self.assertEqual({
            "name": "merge_time_per_shard",
            "value": {
                "min": 40,
                "median": 3702,
                "max": 3900,
                "unit": "ms"
            }
        }, select(metric_list, "merge_time_per_shard"))

        self.assertEqual({
            "name": "merge_count",
            "value": {
                "single": 2
            }
        }, select(metric_list, "merge_count"))

        self.assertEqual({
            "name": "refresh_time",
            "value": {
                "single": 596
            }
        }, select(metric_list, "refresh_time"))

        self.assertEqual({
            "name": "refresh_time_per_shard",
            "value": {
                "min": 48,
                "median": 89,
                "max": 204,
                "unit": "ms"
            }
        }, select(metric_list, "refresh_time_per_shard"))

        self.assertEqual({
            "name": "refresh_count",
            "value": {
                "single": 10
            }
        }, select(metric_list, "refresh_count"))

        self.assertIsNone(select(metric_list, "flush_time"))
        self.assertIsNone(select(metric_list, "flush_time_per_shard"))
        self.assertEqual({
            "name": "flush_count",
            "value": {
                "single": 0
            }
        }, select(metric_list, "flush_count"))


class SystemStatsTests(TestCase):
    def test_as_flat_list(self):
        d = {
            "node_metrics": [
                {
                    "node": "benchmark-node-0",
                    "name": "startup_time",
                    "value": 3.4
                },
                {
                    "node": "benchmark-node-1",
                    "name": "startup_time",
                    "value": 4.2
                },
                {
                    "node": "benchmark-node-0",
                    "name": "index_size",
                    "value": 300 * 1024 * 1024
                },
                {
                    "node": "benchmark-node-1",
                    "name": "index_size",
                    "value": 302 * 1024 * 1024
                },
                {
                    "node": "benchmark-node-0",
                    "name": "bytes_written",
                    "value": 817 * 1024 * 1024
                },
                {
                    "node": "benchmark-node-1",
                    "name": "bytes_written",
                    "value": 833 * 1024 * 1024
                },
            ],
        }

        s = metrics.SystemStats(d)
        metric_list = s.as_flat_list()

        self.assertEqual({
            "node": "benchmark-node-0",
            "name": "startup_time",
            "value": {
                "single": 3.4
            }
        }, select(metric_list, "startup_time", node="benchmark-node-0"))

        self.assertEqual({
            "node": "benchmark-node-1",
            "name": "startup_time",
            "value": {
                "single": 4.2
            }
        }, select(metric_list, "startup_time", node="benchmark-node-1"))

        self.assertEqual({
            "node": "benchmark-node-0",
            "name": "index_size",
            "value": {
                "single": 300 * 1024 * 1024
            }
        }, select(metric_list, "index_size", node="benchmark-node-0"))

        self.assertEqual({
            "node": "benchmark-node-1",
            "name": "index_size",
            "value": {
                "single": 302 * 1024 * 1024
            }
        }, select(metric_list, "index_size", node="benchmark-node-1"))

        self.assertEqual({
            "node": "benchmark-node-0",
            "name": "bytes_written",
            "value": {
                "single": 817 * 1024 * 1024
            }
        }, select(metric_list, "bytes_written", node="benchmark-node-0"))

        self.assertEqual({
            "node": "benchmark-node-1",
            "name": "bytes_written",
            "value": {
                "single": 833 * 1024 * 1024
            }
        }, select(metric_list, "bytes_written", node="benchmark-node-1"))

class TestIndexTemplateProvider:
    def setup_method(self, method):
        self.cfg = config.Config()
        self.cfg.add(config.Scope.application, "node", "root.dir", os.path.join(tempfile.gettempdir(), str(uuid.uuid4())))
        self.cfg.add(config.Scope.application, "node", "benchmark.root", paths.benchmark_root())
        self.cfg.add(config.Scope.application, "system", "env.name", "unittest-env")
        self.cfg.add(config.Scope.application, "system", "list.max_results", 100)

    def test_primary_and_replica_shard_count_specified_index_template_update(self):
        _datastore_type = "opensearch"
        _datastore_number_of_shards = random.randint(1, 100)
        _datastore_number_of_replicas = random.randint(0, 100)

        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_shards", _datastore_number_of_shards)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.metrics_template(),
            _index_template_provider.test_executions_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == _datastore_number_of_shards
            assert t["settings"]["index"]["number_of_replicas"] == _datastore_number_of_replicas

    def test_primary_shard_count_specified_index_template_update(self):
        _datastore_type = "opensearch"
        _datastore_number_of_shards = random.randint(1, 100)

        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_shards", _datastore_number_of_shards)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.metrics_template(),
            _index_template_provider.test_executions_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == _datastore_number_of_shards
            with pytest.raises(KeyError):
                # pylint: disable=unused-variable
                number_of_replicas = t["settings"]["index"]["number_of_replicas"]

    def test_replica_shard_count_specified_index_template_update(self):
        _datastore_type = "opensearch"
        _datastore_number_of_replicas = random.randint(1, 100)

        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.metrics_template(),
            _index_template_provider.test_executions_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_replicas"] == _datastore_number_of_replicas
            with pytest.raises(KeyError):
                # pylint: disable=unused-variable
                number_of_shards = t["settings"]["index"]["number_of_shards"]

    def test_primary_shard_count_less_than_one(self):
        _datastore_type = "opensearch"
        _datastore_number_of_shards = 0

        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_shards", _datastore_number_of_shards)
        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        with pytest.raises(exceptions.SystemSetupError) as ctx:
            # pylint: disable=unused-variable
            templates = [
                _index_template_provider.metrics_template(),
                _index_template_provider.test_executions_template(),
                _index_template_provider.results_template(),
            ]
        assert ctx.value.args[0] == (
            "The setting: datastore.number_of_shards must be >= 1. Please check the configuration in "
            f"{_index_template_provider._config.config_file.location}"
        )

    def test_primary_and_replica_shard_counts_passed_as_strings(self):
        _datastore_type = "opensearch"
        _datastore_number_of_shards = "200"
        _datastore_number_of_replicas = "1"

        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.type", _datastore_type)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_shards", _datastore_number_of_shards)
        self.cfg.add(config.Scope.applicationOverride, "results_publishing", "datastore.number_of_replicas", _datastore_number_of_replicas)

        _index_template_provider = metrics.IndexTemplateProvider(self.cfg)

        templates = [
            _index_template_provider.metrics_template(),
            _index_template_provider.test_executions_template(),
            _index_template_provider.results_template(),
        ]

        for template in templates:
            t = json.loads(template)
            assert t["settings"]["index"]["number_of_shards"] == 200
            assert t["settings"]["index"]["number_of_replicas"] == 1
