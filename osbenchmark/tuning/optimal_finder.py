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

import os
import sys
import csv
import uuid
import logging
import tempfile
import subprocess
from datetime import datetime
from timeit import default_timer as timer
from osbenchmark.tuning.schedule import BatchSizeSchedule, BulkSizeSchedule, ClientSchedule, ScheduleRunner
from osbenchmark.tuning.result import Result


def get_benchmark_params(args, batch_size, bulk_size, number_of_client, temp_output_file):
    params = {}
    params["--target-hosts"] = args.target_hosts
    if args.client_options:
        params["--client-options"] = args.client_options
    params["--kill-running-processes"] = None
    # we only test remote cluster
    params["--pipeline"] = "benchmark-only"
    params["--telemetry"] = "node-stats"
    params["--telemetry-params"] = ("node-stats-include-indices:true,"
                                    "node-stats-sample-interval:10,"
                                    "node-stats-include-mem:true,"
                                    "node-stats-include-process:true")
    params["--workload-path"] = args.workload_path
    params["--workload-params"] = get_workload_params(batch_size, bulk_size, number_of_client)
    # generate output
    params["--results-format"] = "csv"
    params["--results-file"] = temp_output_file
    return params


def get_workload_params(batch_size, bulk_size, number_of_client):
    params = [f"bulk_size:{bulk_size}",
              f"batch_size:{batch_size}",
              f"bulk_indexing_clients:{number_of_client}",
              f"index_name:{generate_random_index_name()}"]

    return ",".join(params)


def run_benchmark(params):
    commands = ["opensearch-benchmark", "execute-test"]
    for k, v in params.items():
        commands.append(k)
        if v:
            commands.append(v)

    proc = None
    try:
        proc = subprocess.Popen(
            commands,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)

        _, stderr = proc.communicate()
        return proc.returncode == 0, stderr.decode('ascii')
    except KeyboardInterrupt as e:
        proc.terminate()
        print("Process is terminated!")
        raise e


def generate_random_index_name():
    return str(datetime.now().timestamp()) + "_" + str(uuid.uuid4())


def run_batch_bulk_client_tests(args, test_id, batch, bulk, client):
    logger = logging.getLogger(__name__)
    result = Result(test_id, batch, bulk, client)
    _, filename = tempfile.mkstemp()
    params = get_benchmark_params(args, batch, bulk, client, filename)

    logger.info("test_id: %s, batch: %d, bulk: %d, client: %d", test_id, batch, bulk, client)
    success = False
    err = None
    start = timer()
    try:
        success, err = run_benchmark(params)
    finally:
        end = timer()
        if success:
            with open(filename, 'r', newline='') as csvfile:
                line_reader = csv.reader(csvfile, delimiter=',')
                output = {}
                for row in line_reader:
                    output[row[0]] = row[2]
                result.set_output(True, int(end - start), output)
        else:
            logger.error(err)
            result.set_output(False, int(end - start), None)

    if os.path.exists(filename):
        os.remove(filename)

    return result, success, err


def batch_bulk_client_tuning(args):
    batch_schedule = BatchSizeSchedule(args)
    bulk_schedule = BulkSizeSchedule(args)
    client_schedule = ClientSchedule(args)
    batches = batch_schedule.steps
    bulks = bulk_schedule.steps
    number_of_clients = client_schedule.steps

    total = len(batches) * len(bulks) * len(number_of_clients)
    print(f"There will be {total} tests to run with {len(batches)} batch sizes, { len(bulks)} bulk sizes, "
          f"{len(number_of_clients)} client numbers.")

    schedule_runner = ScheduleRunner(args, batch_schedule, bulk_schedule, client_schedule)
    results = schedule_runner.run(run_batch_bulk_client_tests)

    successful_result_ids = get_successful_ids(results, float(args.allowed_error_rate))
    optimal = find_optimal_result([results[result_id] for result_id in successful_result_ids])
    if not optimal:
        print("All tests failed, couldn't find any results!")
    else:
        print(f"the optimal variable combination is: bulk size: {optimal.bulk_size}, "
              f"batch size: {optimal.batch_size}, number of clients: {optimal.number_of_client}")
    return results


def get_successful_ids(results, allowed_error_rate):
    successful_ids = []
    for result in results:
        if result.success and result.error_rate <= allowed_error_rate:
            successful_ids.append(result.test_id)
    return successful_ids


def find_optimal_result(results):
    total_time = sys.maxsize
    optimal = None
    for result in results:
        if result.total_time < total_time:
            total_time = result.total_time
            optimal = result
    return optimal


def run(args):
    batch_bulk_client_tuning(args)
