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

from functools import partial
import csv
import io
import logging
import sys
import re
from enum import Enum

import tabulate

from osbenchmark import metrics, exceptions
from osbenchmark.utils import convert, io as rio, console

FINAL_SCORE = r"""
------------------------------------------------------
    _______             __   _____
   / ____(_)___  ____ _/ /  / ___/_________  ________
  / /_  / / __ \/ __ `/ /   \__ \/ ___/ __ \/ ___/ _ \
 / __/ / / / / / /_/ / /   ___/ / /__/ /_/ / /  /  __/
/_/   /_/_/ /_/\__,_/_/   /____/\___/\____/_/   \___/
------------------------------------------------------
            """

class Throughput(Enum):
    MEAN = "mean"
    MAX = "max"
    MIN = "min"
    MEDIAN = "median"

def summarize(results, cfg):
    SummaryResultsPublisher(results, cfg).publish()


def compare(cfg, baseline_id, contender_id):
    if not baseline_id or not contender_id:
        raise exceptions.SystemSetupError("compare needs baseline and a contender")
    test_execution_store = metrics.test_execution_store(cfg)
    ComparisonResultsPublisher(cfg).publish(
        test_execution_store.find_by_test_execution_id(baseline_id),
        test_execution_store.find_by_test_execution_id(contender_id))


def print_internal(message):
    console.println(message, logger=logging.getLogger(__name__).info)


def print_header(message):
    print_internal(console.format.bold(message))


def write_single_results(results_file, results_format, cwd, numbers_align, headers, data_plain, data_rich):
    if results_format == "markdown":
        formatter = partial(format_as_markdown, numbers_align=numbers_align)
    elif results_format == "csv":
        formatter = format_as_csv
    else:
        raise exceptions.SystemSetupError("Unknown publish format '%s'" % results_format)

    print_internal(formatter(headers, data_rich))
    if len(results_file) > 0:
        normalized_results_file = rio.normalize_path(results_file, cwd)
        # ensure that the parent folder already exists when we try to write the file...
        rio.ensure_dir(rio.dirname(normalized_results_file))
        with open(normalized_results_file, mode="a+", encoding="utf-8") as f:
            f.writelines(formatter(headers, data_plain))


def format_as_markdown(headers, data, numbers_align):
    rendered = tabulate.tabulate(data, headers=headers, tablefmt="pipe", numalign=numbers_align, stralign="right")
    return rendered + "\n"


def format_as_csv(headers, data):
    with io.StringIO() as out:
        writer = csv.writer(out)
        writer.writerow(headers)
        for metric_record in data:
            writer.writerow(metric_record)
        return out.getvalue()

def comma_separated_string_to_number_list(string_list):
    # Split a comma-separated list in a string to a list of numbers. If they are whole numbers, make them ints,
    # so they display without decimals.
    # If the input is None, return None.
    if string_list is None or len(string_list) == 0:
        return None
    results = [float(value) for value in string_list.split(",")]
    for i, value in enumerate(results):
        if round(value) == value:
            results[i] = int(value)
    return results



class SummaryResultsPublisher:
    def __init__(self, results, config):
        self.results = results
        self.results_file = config.opts("results_publishing", "output.path")
        self.results_format = config.opts("results_publishing", "format")
        self.numbers_align = config.opts("results_publishing", "numbers.align",
                                         mandatory=False, default_value="right")
        results_publishing_values = config.opts("results_publishing", "values")
        self.publish_all_values = results_publishing_values == "all"
        self.publish_all_percentile_values = results_publishing_values == "all-percentiles"
        self.show_processing_time = convert.to_bool(config.opts("results_publishing", "output.processingtime",
                                                                mandatory=False, default_value=False))
        self.cwd = config.opts("node", "benchmark.cwd")
        self.display_percentiles = {
            "throughput":comma_separated_string_to_number_list(config.opts("workload", "throughput.percentiles", mandatory=False)),
            "latency": comma_separated_string_to_number_list(config.opts("workload", "latency.percentiles", mandatory=False))
        }
        self.logger = logging.getLogger(__name__)

    def publish_operational_statistics(self, metrics_table: list, warnings: list, record, task):
        metrics_table.extend(self._publish_throughput(record, task))
        metrics_table.extend(self._publish_latency(record, task))
        metrics_table.extend(self._publish_service_time(record, task))
        # this is mostly needed for debugging purposes but not so relevant to end users
        if self.show_processing_time:
            metrics_table.extend(self._publish_processing_time(record, task))
        metrics_table.extend(self._publish_error_rate(record, task))
        self.add_warnings(warnings, record, task)

    def publish(self):
        print_header(FINAL_SCORE)

        stats = self.results

        warnings = []
        metrics_table = []
        metrics_table.extend(self._publish_totals(stats))
        metrics_table.extend(self._publish_ml_processing_times(stats))

        metrics_table.extend(self._publish_gc_metrics(stats))

        metrics_table.extend(self._publish_disk_usage(stats))
        metrics_table.extend(self._publish_segment_memory(stats))
        metrics_table.extend(self._publish_segment_counts(stats))

        metrics_table.extend(self._publish_transform_stats(stats))

        # These variables are used with the clients_list parameter in test_procedures to find the max throughput.
        max_throughput = -1
        record_with_best_throughput = None

        throughput_pattern = r"_(\d+)_clients$"


        for record in stats.op_metrics:
            task = record["task"]
            is_task_part_of_throughput_testing = re.search(throughput_pattern, task)
            if is_task_part_of_throughput_testing:
                # assumption: all units are the same and only maximizing throughput over one operation (i.e. not both ingest and search).
                # To maximize throughput over multiple operations, would need a list/dictionary of maximum throughputs.
                task_throughput = record["throughput"][Throughput.MEAN.value]
                self.logger.info("Task %s has throughput %s", task, task_throughput)
                if task_throughput > max_throughput:
                    max_throughput = task_throughput
                    record_with_best_throughput = record

            else:
                self.publish_operational_statistics(metrics_table=metrics_table, warnings=warnings, record=record, task=task)

        # The following code is run when the clients_list parameter is specified and publishes the max throughput.
        if max_throughput != -1 and record_with_best_throughput is not None:
            self.publish_operational_statistics(metrics_table=metrics_table, warnings=warnings, record=record_with_best_throughput,
                                                task=record_with_best_throughput["task"])
            metrics_table.extend(self._publish_best_client_settings(record_with_best_throughput, record_with_best_throughput["task"]))

        for record in stats.correctness_metrics:
            task = record["task"]

            keys = record.keys()
            recall_keys_in_task_dict = "recall@1" in keys and "recall@k" in keys
            if recall_keys_in_task_dict and "mean" in record["recall@1"] and "mean" in record["recall@k"]:
                metrics_table.extend(self._publish_recall(record, task))

        for record in stats.profile_metrics:
            task = record["task"]
            metrics_table.extend(self._publish_profile_metrics(record["metrics"], task))

        self.write_results(metrics_table)

        if warnings:
            for warning in warnings:
                console.warn(warning)

    def add_warnings(self, warnings, values, op):
        if values["error_rate"] > 0:
            warnings.append(f"Error rate is {round(values['error_rate'] * 100, 2)} for operation '{op}'. Please check the logs.")
        if values["throughput"]["median"] is None:
            error_rate = values["error_rate"]
            if error_rate:
                warnings.append("No throughput metrics available for [%s]. Likely cause: Error rate is %.1f%%. Please check the logs."
                                % (op, error_rate * 100))
            else:
                warnings.append("No throughput metrics available for [%s]. Likely cause: The benchmark ended already during warmup." % op)

    def write_results(self, metrics_table):
        write_single_results(self.results_file, self.results_format, self.cwd, self.numbers_align,
                            headers=["Metric", "Task", "Value", "Unit"],
                            data_plain=metrics_table,
                            data_rich=metrics_table)

    def _publish_throughput(self, values, task):
        throughput = values["throughput"]
        unit = throughput["unit"]

        return self._join(
            self._line("Min Throughput", task, throughput["min"], unit, lambda v: "%.2f" % v),
            self._line("Mean Throughput", task, throughput["mean"], unit, lambda v: "%.2f" % v),
            self._line("Median Throughput", task, throughput["median"], unit, lambda v: "%.2f" % v),
            self._line("Max Throughput", task, throughput["max"], unit, lambda v: "%.2f" % v),
            *self._publish_percentiles("throughput", task, throughput)
        )

    def _publish_latency(self, values, task):
        return self._publish_percentiles("latency", task, values["latency"])

    def _publish_service_time(self, values, task):
        return self._publish_percentiles("service time", task, values["service_time"])

    def _publish_processing_time(self, values, task):
        return self._publish_percentiles("processing time", task, values["processing_time"])

    def _publish_recall(self, values, task):
        recall_k_mean = values["recall@k"]["mean"]
        recall_1_mean = values["recall@1"]["mean"]

        return self._join(
            self._line("Mean recall@k", task, recall_k_mean, "", lambda v: "%.2f" % v),
            self._line("Mean recall@1", task, recall_1_mean, "", lambda v: "%.2f" % v)
        )

    def _publish_profile_metrics(self, metrics, task):
        percentiles = [self._publish_percentiles(key, task, value) for key, value in metrics.items()]

        return self._join(
            *[item for percentile in percentiles for item in percentile]
        )

    def _publish_best_client_settings(self, record, task):
        num_clients = re.search(r"_(\d+)_clients$", task).group(1)
        return self._join(self._line("Number of clients that achieved max throughput", "", num_clients, ""))

    def _publish_percentiles(self, name, task, value, unit="ms"):
        lines = []
        percentiles = self.display_percentiles.get(name, metrics.GlobalStatsCalculator.OTHER_PERCENTILES)

        if value:
            for percentile in metrics.percentiles_for_sample_size(sys.maxsize, percentiles_list=percentiles):
                percentile_value = value.get(metrics.encode_float_key(percentile))
                a_line = self._line("%sth percentile %s" % (percentile, name), task, percentile_value, unit,
                                    force=self.publish_all_percentile_values)
                self._append_non_empty(lines, a_line)
        return lines

    def _publish_error_rate(self, values, task):
        return self._join(
            self._line("error rate", task, values["error_rate"], "%", lambda v: "%.2f" % (v * 100.0))
        )

    def _publish_totals(self, stats):
        lines = []
        lines.extend(self._publish_total_time("indexing time", stats.total_time))
        lines.extend(self._publish_total_time_per_shard("indexing time", stats.total_time_per_shard))
        lines.extend(self._publish_total_time("indexing throttle time", stats.indexing_throttle_time))
        lines.extend(self._publish_total_time_per_shard("indexing throttle time", stats.indexing_throttle_time_per_shard))
        lines.extend(self._publish_total_time("merge time", stats.merge_time))
        lines.extend(self._publish_total_count("merge count", stats.merge_count))
        lines.extend(self._publish_total_time_per_shard("merge time", stats.merge_time_per_shard))
        lines.extend(self._publish_total_time("merge throttle time", stats.merge_throttle_time))
        lines.extend(self._publish_total_time_per_shard("merge throttle time", stats.merge_throttle_time_per_shard))
        lines.extend(self._publish_total_time("refresh time", stats.refresh_time))
        lines.extend(self._publish_total_count("refresh count", stats.refresh_count))
        lines.extend(self._publish_total_time_per_shard("refresh time", stats.refresh_time_per_shard))
        lines.extend(self._publish_total_time("flush time", stats.flush_time))
        lines.extend(self._publish_total_count("flush count", stats.flush_count))
        lines.extend(self._publish_total_time_per_shard("flush time", stats.flush_time_per_shard))
        return lines

    def _publish_total_time(self, name, total_time):
        unit = "min"
        return self._join(
            self._line("Cumulative {} of primary shards".format(name), "", total_time, unit, convert.ms_to_minutes),
        )

    def _publish_total_time_per_shard(self, name, total_time_per_shard):
        unit = "min"
        return self._join(
            self._line("Min cumulative {} across primary shards".format(name), "", total_time_per_shard.get("min"), unit,
                       convert.ms_to_minutes),
            self._line("Median cumulative {} across primary shards".format(name), "", total_time_per_shard.get("median"),
                       unit, convert.ms_to_minutes),
            self._line("Max cumulative {} across primary shards".format(name), "", total_time_per_shard.get("max"), unit,
                       convert.ms_to_minutes),
        )

    def _publish_total_count(self, name, total_count):
        return self._join(
            self._line("Cumulative {} of primary shards".format(name), "", total_count, ""),
        )

    def _publish_ml_processing_times(self, stats):
        lines = []
        for processing_time in stats.ml_processing_time:
            job_name = processing_time["job"]
            unit = processing_time["unit"]
            lines.append(self._line("Min ML processing time", job_name, processing_time["min"], unit))
            lines.append(self._line("Mean ML processing time", job_name, processing_time["mean"], unit))
            lines.append(self._line("Median ML processing time", job_name, processing_time["median"], unit))
            lines.append(self._line("Max ML processing time", job_name, processing_time["max"], unit))
        return lines

    def _publish_gc_metrics(self, stats):
        return self._join(
            self._line("Total Young Gen GC time", "", stats.young_gc_time, "s", convert.ms_to_seconds),
            self._line("Total Young Gen GC count", "", stats.young_gc_count, ""),
            self._line("Total Old Gen GC time", "", stats.old_gc_time, "s", convert.ms_to_seconds),
            self._line("Total Old Gen GC count", "", stats.old_gc_count, "")
        )

    def _publish_disk_usage(self, stats):
        return self._join(
            self._line("Store size", "", stats.store_size, "GB", convert.bytes_to_gb),
            self._line("Translog size", "", stats.translog_size, "GB", convert.bytes_to_gb),
        )

    def _publish_segment_memory(self, stats):
        unit = "MB"
        return self._join(
            self._line("Heap used for segments", "", stats.memory_segments, unit, convert.bytes_to_mb),
            self._line("Heap used for doc values", "", stats.memory_doc_values, unit, convert.bytes_to_mb),
            self._line("Heap used for terms", "", stats.memory_terms, unit, convert.bytes_to_mb),
            self._line("Heap used for norms", "", stats.memory_norms, unit, convert.bytes_to_mb),
            self._line("Heap used for points", "", stats.memory_points, unit, convert.bytes_to_mb),
            self._line("Heap used for stored fields", "", stats.memory_stored_fields, unit, convert.bytes_to_mb)
        )

    def _publish_segment_counts(self, stats):
        return self._join(
            self._line("Segment count", "", stats.segment_count, "")
        )

    def _publish_transform_stats(self, stats):
        lines = []
        for processing_time in stats.total_transform_processing_times:
            lines.append(
                self._line("Transform processing time", processing_time["id"], processing_time["mean"],
                           processing_time["unit"]))
        for index_time in stats.total_transform_index_times:
            lines.append(
                self._line("Transform indexing time", index_time["id"], index_time["mean"], index_time["unit"]))
        for search_time in stats.total_transform_search_times:
            lines.append(
                self._line("Transform search time", search_time["id"], search_time["mean"], search_time["unit"]))
        for throughput in stats.total_transform_throughput:
            lines.append(
                self._line("Transform throughput", throughput["id"], throughput["mean"], throughput["unit"]))

        return lines

    def _join(self, *args):
        lines = []
        for arg in args:
            self._append_non_empty(lines, arg)
        return lines

    def _append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def _line(self, k, task, v, unit, converter=lambda x: x, force=False):
        if v is not None or force or self.publish_all_values:
            u = unit if v is not None else None
            return [k, task, converter(v), u]
        else:
            return []


class ComparisonResultsPublisher:
    def __init__(self, config):
        self.logger = logging.getLogger(__name__)
        self.results_file = config.opts("results_publishing", "output.path")
        self.results_format = config.opts("results_publishing", "format")
        self.numbers_align = config.opts("results_publishing", "numbers.align",
                                         mandatory=False, default_value="right")
        self.cwd = config.opts("node", "benchmark.cwd")
        self.show_processing_time = convert.to_bool(config.opts("results_publishing", "output.processingtime",
                                                                mandatory=False, default_value=False))
        self.percentiles = comma_separated_string_to_number_list(config.opts("results_publishing", "percentiles", mandatory=False))
        self.plain = False

    def publish(self, r1, r2):
        # we don't verify anything about the test_executions as it is possible
        # that the user benchmarks two different workloads intentionally
        baseline_stats = metrics.GlobalStats(r1.results)
        contender_stats = metrics.GlobalStats(r2.results)

        print_internal("")
        print_internal("Comparing baseline")
        print_internal("  TestExecution ID: %s" % r1.test_execution_id)
        print_internal("  TestExecution timestamp: %s" % r1.test_execution_timestamp)
        if r1.test_procedure_name:
            print_internal("  TestProcedure: %s" % r1.test_procedure_name)
        print_internal("  ProvisionConfigInstance: %s" % r1.provision_config_instance_name)
        if r1.user_tags:
            r1_user_tags = ", ".join(["%s=%s" % (k, v) for k, v in sorted(r1.user_tags.items())])
            print_internal("  User tags: %s" % r1_user_tags)
        print_internal("")
        print_internal("with contender")
        print_internal("  TestExecution ID: %s" % r2.test_execution_id)
        print_internal("  TestExecution timestamp: %s" % r2.test_execution_timestamp)
        if r2.test_procedure_name:
            print_internal("  TestProcedure: %s" % r2.test_procedure_name)
        print_internal("  ProvisionConfigInstance: %s" % r2.provision_config_instance_name)
        if r2.user_tags:
            r2_user_tags = ", ".join(["%s=%s" % (k, v) for k, v in sorted(r2.user_tags.items())])
            print_internal("  User tags: %s" % r2_user_tags)
        print_header(FINAL_SCORE)

        metric_table_plain = self._metrics_table(baseline_stats, contender_stats, plain=True)
        metric_table_rich = self._metrics_table(baseline_stats, contender_stats, plain=False)
        # Writes metric_table_rich to console, writes metric_table_plain to file
        self._write_results(metric_table_plain, metric_table_rich)

    def _metrics_table(self, baseline_stats, contender_stats, plain):
        self.plain = plain
        metrics_table = []
        metrics_table.extend(self._publish_total_times(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_ml_processing_times(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_gc_metrics(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_disk_usage(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_segment_memory(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_segment_counts(baseline_stats, contender_stats))
        metrics_table.extend(self._publish_transform_processing_times(baseline_stats, contender_stats))

        for t in baseline_stats.tasks():
            if t in contender_stats.tasks():
                metrics_table.extend(self._publish_throughput(baseline_stats, contender_stats, t))
                metrics_table.extend(self._publish_latency(baseline_stats, contender_stats, t))
                metrics_table.extend(self._publish_service_time(baseline_stats, contender_stats, t))
                if self.show_processing_time:
                    metrics_table.extend(self._publish_processing_time(baseline_stats, contender_stats, t))
                metrics_table.extend(self._publish_error_rate(baseline_stats, contender_stats, t))
        return metrics_table

    def _write_results(self, metrics_table, metrics_table_console):
        write_single_results(self.results_file, self.results_format, self.cwd, self.numbers_align,
                            headers=["Metric", "Task", "Baseline", "Contender", "Diff", "Unit"],
                            data_plain=metrics_table, data_rich=metrics_table_console)

    def _publish_throughput(self, baseline_stats, contender_stats, task):
        b_min = baseline_stats.metrics(task)["throughput"].get("overall_min") or baseline_stats.metrics(task)["throughput"]["min"]
        b_mean = baseline_stats.metrics(task)["throughput"]["mean"]
        b_median = baseline_stats.metrics(task)["throughput"]["median"]
        b_max = baseline_stats.metrics(task)["throughput"].get("overall_max") or baseline_stats.metrics(task)["throughput"]["max"]
        b_unit = baseline_stats.metrics(task)["throughput"]["unit"]

        c_min = contender_stats.metrics(task)["throughput"].get("overall_min") or contender_stats.metrics(task)["throughput"]["min"]
        c_mean = contender_stats.metrics(task)["throughput"]["mean"]
        c_median = contender_stats.metrics(task)["throughput"]["median"]
        c_max = contender_stats.metrics(task)["throughput"].get("overall_max") or contender_stats.metrics(task)["throughput"]["max"]

        return self._join(
            self._line("Min Throughput", b_min, c_min, task, b_unit, treat_increase_as_improvement=True),
            self._line("Mean Throughput", b_mean, c_mean, task, b_unit, treat_increase_as_improvement=True),
            self._line("Median Throughput", b_median, c_median, task, b_unit, treat_increase_as_improvement=True),
            self._line("Max Throughput", b_max, c_max, task, b_unit, treat_increase_as_improvement=True)
        )

    def _publish_latency(self, baseline_stats, contender_stats, task):
        baseline_latency = baseline_stats.metrics(task)["latency"]
        contender_latency = contender_stats.metrics(task)["latency"]
        return self._publish_percentiles("latency", task, baseline_latency, contender_latency)

    def _publish_service_time(self, baseline_stats, contender_stats, task):
        baseline_service_time = baseline_stats.metrics(task)["service_time"]
        contender_service_time = contender_stats.metrics(task)["service_time"]
        return self._publish_percentiles("service time", task, baseline_service_time, contender_service_time)

    def _publish_processing_time(self, baseline_stats, contender_stats, task):
        baseline_processing_time = baseline_stats.metrics(task)["processing_time"]
        contender_processing_time = contender_stats.metrics(task)["processing_time"]
        return self._publish_percentiles("processing time", task, baseline_processing_time, contender_processing_time)

    def _publish_percentiles(self, name, task, baseline_values, contender_values):
        lines = []
        for percentile in metrics.percentiles_for_sample_size(sys.maxsize, percentiles_list=self.percentiles):
            baseline_value = baseline_values.get(metrics.encode_float_key(percentile))
            contender_value = contender_values.get(metrics.encode_float_key(percentile))
            self._append_non_empty(lines, self._line("%sth percentile %s" % (percentile, name),
                                                     baseline_value, contender_value, task, "ms",
                                                     treat_increase_as_improvement=False))
        return lines

    def _publish_error_rate(self, baseline_stats, contender_stats, task):
        baseline_error_rate = baseline_stats.metrics(task)["error_rate"]
        contender_error_rate = contender_stats.metrics(task)["error_rate"]
        return self._join(
            self._line("error rate", baseline_error_rate, contender_error_rate, task, "%",
                       treat_increase_as_improvement=False, formatter=convert.factor(100.0))
        )

    def _publish_ml_processing_times(self, baseline_stats, contender_stats):
        lines = []
        for baseline in baseline_stats.ml_processing_time:
            job_name = baseline["job"]
            unit = baseline["unit"]
            # O(n^2) but we assume here only a *very* limited number of jobs (usually just one)
            for contender in contender_stats.ml_processing_time:
                if contender["job"] == job_name:
                    lines.append(self._line("Min ML processing time", baseline["min"], contender["min"],
                                            job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self._line("Mean ML processing time", baseline["mean"], contender["mean"],
                                            job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self._line("Median ML processing time", baseline["median"], contender["median"],
                                            job_name, unit, treat_increase_as_improvement=False))
                    lines.append(self._line("Max ML processing time", baseline["max"], contender["max"],
                                            job_name, unit, treat_increase_as_improvement=False))
        return lines

    def _publish_transform_processing_times(self, baseline_stats, contender_stats):
        lines = []
        if baseline_stats.total_transform_processing_times is None:
            return lines
        for baseline in baseline_stats.total_transform_processing_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_processing_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line("Transform processing time", baseline["mean"], contender["mean"],
                                   transform_id, baseline["unit"], treat_increase_as_improvement=True))
        for baseline in baseline_stats.total_transform_index_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_index_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line("Transform indexing time", baseline["mean"], contender["mean"],
                                   transform_id, baseline["unit"], treat_increase_as_improvement=True))
        for baseline in baseline_stats.total_transform_search_times:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_search_times:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line("Transform search time", baseline["mean"], contender["mean"],
                                   transform_id, baseline["unit"], treat_increase_as_improvement=True))
        for baseline in baseline_stats.total_transform_throughput:
            transform_id = baseline["id"]
            for contender in contender_stats.total_transform_throughput:
                if contender["id"] == transform_id:
                    lines.append(
                        self._line("Transform throughput", baseline["mean"], contender["mean"],
                                   transform_id, baseline["unit"], treat_increase_as_improvement=True))
        return lines

    def _publish_total_times(self, baseline_stats, contender_stats):
        lines = []
        lines.extend(self._publish_total_time(
            "indexing time",
            baseline_stats.total_time, contender_stats.total_time
        ))
        lines.extend(self._publish_total_time_per_shard(
            "indexing time",
            baseline_stats.total_time_per_shard, contender_stats.total_time_per_shard
        ))
        lines.extend(self._publish_total_time(
            "indexing throttle time",
            baseline_stats.indexing_throttle_time, contender_stats.indexing_throttle_time
        ))
        lines.extend(self._publish_total_time_per_shard(
            "indexing throttle time",
            baseline_stats.indexing_throttle_time_per_shard,
            contender_stats.indexing_throttle_time_per_shard
        ))
        lines.extend(self._publish_total_time(
            "merge time",
            baseline_stats.merge_time, contender_stats.merge_time,
        ))
        lines.extend(self._publish_total_count(
            "merge count",
            baseline_stats.merge_count, contender_stats.merge_count
        ))
        lines.extend(self._publish_total_time_per_shard(
            "merge time",
            baseline_stats.merge_time_per_shard,
            contender_stats.merge_time_per_shard
        ))
        lines.extend(self._publish_total_time(
            "merge throttle time",
            baseline_stats.merge_throttle_time,
            contender_stats.merge_throttle_time
        ))
        lines.extend(self._publish_total_time_per_shard(
            "merge throttle time",
            baseline_stats.merge_throttle_time_per_shard,
            contender_stats.merge_throttle_time_per_shard
        ))
        lines.extend(self._publish_total_time(
            "refresh time",
            baseline_stats.refresh_time, contender_stats.refresh_time
        ))
        lines.extend(self._publish_total_count(
            "refresh count",
            baseline_stats.refresh_count, contender_stats.refresh_count
        ))
        lines.extend(self._publish_total_time_per_shard(
            "refresh time",
            baseline_stats.refresh_time_per_shard,
            contender_stats.refresh_time_per_shard
        ))
        lines.extend(self._publish_total_time(
            "flush time",
            baseline_stats.flush_time, contender_stats.flush_time
        ))
        lines.extend(self._publish_total_count(
            "flush count",
            baseline_stats.flush_count, contender_stats.flush_count
        ))
        lines.extend(self._publish_total_time_per_shard(
            "flush time",
            baseline_stats.flush_time_per_shard, contender_stats.flush_time_per_shard
        ))
        return lines

    def _publish_total_time(self, name, baseline_total, contender_total):
        unit = "min"
        return self._join(
            self._line("Cumulative {} of primary shards".format(name), baseline_total, contender_total, "", unit,
                       treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
        )

    def _publish_total_time_per_shard(self, name, baseline_per_shard, contender_per_shard):
        unit = "min"
        return self._join(
            self._line("Min cumulative {} across primary shard".format(name), baseline_per_shard.get("min"),
                       contender_per_shard.get("min"), "", unit, treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self._line("Median cumulative {} across primary shard".format(name), baseline_per_shard.get("median"),
                       contender_per_shard.get("median"), "", unit, treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
            self._line("Max cumulative {} across primary shard".format(name), baseline_per_shard.get("max"), contender_per_shard.get("max"),
                      "", unit, treat_increase_as_improvement=False, formatter=convert.ms_to_minutes),
        )

    def _publish_total_count(self, name, baseline_total, contender_total):
        return self._join(
            self._line("Cumulative {} of primary shards".format(name), baseline_total, contender_total, "", "",
                       treat_increase_as_improvement=False)
        )

    def _publish_gc_metrics(self, baseline_stats, contender_stats):
        return self._join(
            self._line("Total Young Gen GC time", baseline_stats.young_gc_time, contender_stats.young_gc_time, "", "s",
                       treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self._line("Total Young Gen GC count", baseline_stats.young_gc_count, contender_stats.young_gc_count, "", "",
                       treat_increase_as_improvement=False),
            self._line("Total Old Gen GC time", baseline_stats.old_gc_time, contender_stats.old_gc_time, "", "s",
                       treat_increase_as_improvement=False, formatter=convert.ms_to_seconds),
            self._line("Total Old Gen GC count", baseline_stats.old_gc_count, contender_stats.old_gc_count, "", "",
                       treat_increase_as_improvement=False)
        )

    def _publish_disk_usage(self, baseline_stats, contender_stats):
        return self._join(
            self._line("Store size", baseline_stats.store_size, contender_stats.store_size, "", "GB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
            self._line("Translog size", baseline_stats.translog_size, contender_stats.translog_size, "", "GB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_gb),
        )

    def _publish_segment_memory(self, baseline_stats, contender_stats):
        return self._join(
            self._line("Heap used for segments", baseline_stats.memory_segments, contender_stats.memory_segments, "", "MB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
            self._line("Heap used for doc values", baseline_stats.memory_doc_values, contender_stats.memory_doc_values, "", "MB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
            self._line("Heap used for terms", baseline_stats.memory_terms, contender_stats.memory_terms, "", "MB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
            self._line("Heap used for norms", baseline_stats.memory_norms, contender_stats.memory_norms, "", "MB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
            self._line("Heap used for points", baseline_stats.memory_points, contender_stats.memory_points, "", "MB",
                       treat_increase_as_improvement=False, formatter=convert.bytes_to_mb),
            self._line("Heap used for stored fields", baseline_stats.memory_stored_fields, contender_stats.memory_stored_fields, "",
                       "MB", treat_increase_as_improvement=False, formatter=convert.bytes_to_mb)
            )

    def _publish_segment_counts(self, baseline_stats, contender_stats):
        return self._join(
            self._line("Segment count", baseline_stats.segment_count, contender_stats.segment_count,
                       "", "", treat_increase_as_improvement=False)
        )

    def _join(self, *args):
        lines = []
        for arg in args:
            self._append_non_empty(lines, arg)
        return lines

    def _append_non_empty(self, lines, line):
        if line and len(line) > 0:
            lines.append(line)

    def _line(self, metric, baseline, contender, task, unit, treat_increase_as_improvement, formatter=lambda x: x):
        if baseline is not None and contender is not None:
            return [metric, str(task), formatter(baseline), formatter(contender),
                    *self._diff(baseline, contender, treat_increase_as_improvement, formatter), unit]
        else:
            return []

    def _diff(self, baseline, contender, treat_increase_as_improvement, formatter=lambda x: x):
        def identity(x):
            return x

        # Avoid division by zero
        if baseline == 0:
            percentage_diff = 0
        else:
            # Calculate percentage difference: ((new - old) / old) * 100
            percentage_diff = ((contender - baseline) / baseline) * 100
            percentage_diff = formatter(percentage_diff)
        diff = formatter(contender - baseline)

        if self.plain:
            color_greater = identity
            color_smaller = identity
            color_neutral = identity
        elif treat_increase_as_improvement:
            color_greater = console.format.green
            color_smaller = console.format.red
            color_neutral = console.format.neutral
        else:
            color_greater = console.format.red
            color_smaller = console.format.green
            color_neutral = console.format.neutral

        if percentage_diff > 5.0:
            return color_greater("+%.2f%%" % percentage_diff)+" :red_circle:",color_greater("+%.5f" % diff)
        elif percentage_diff < -5.0:
            return color_smaller("%.2f%%" % percentage_diff)+" :green_circle:",color_smaller("%.5f" % diff)
        else:
            # tabulate needs this to align all values correctly
            return color_neutral("%.2f%%" % percentage_diff),color_neutral("%.5f" % diff)
