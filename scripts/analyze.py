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

# Simple helper script to create graphs based on multiple
# test_execution.json files (it's a summary of the results of
# a single test_execution which is
# stored in ~/.benchmark/benchmarks/test_executions/TEST_EXECUTION_TS/).
# There is no specific integration into OSB and it is also not
# installed with OSB.
#
# It requires matplotlib (install with pip3 install matplotlib).
#
#
# Usage:
# python3 analyze.py [--label=LABEL] /path1/to/test_execution.json /path2/to/test_execution.json
#
# Output: A bunch of .png files in the current directory.
# Each graph shows one data series per test_execution.
# The label key is chosen based on the
# command line parameter `--label`
#


import argparse
import json
import sys

try:
    import matplotlib.pyplot as plt
except ImportError:
    print("This script requires matplotlib. Please install with 'pip3 install matplotlib' and retry.", file=sys.stderr)
    sys.exit(1)


def create_plot():
    plt.rcdefaults()
    fig, ax = plt.subplots()
    fig.set_size_inches(18, 10)
    return fig, ax


def present(a_plot, name):
    a_plot.savefig("%s.png" % name, bbox_inches='tight')
    # plt.show()  # alternatively only show it
    # explicitly close to free resources
    a_plot.close()


def decode_percentile_key(k):
    return float(k.replace("_", "."))


def data_series_name(d, label_key):
    data_series = []
    for lbl in label_key.split(","):
        path = lbl.split(".")
        doc = d
        for k in path:
            doc = doc[k]
        data_series.append(doc)
    return ",".join(data_series)


def include(series):
    return True


def plot_service_time(raw_data, label_key):
    service_time_per_op = {}

    for d in raw_data:
        data_series = data_series_name(d, label_key)
        for op_metrics in d["results"]["op_metrics"]:
            operation = op_metrics["operation"]
            service_time_metrics = op_metrics["service_time"]
            if operation not in service_time_per_op:
                service_time_per_op[operation] = []
            service_time_per_op[operation].append({
                "data_series": data_series,
                "percentiles": [decode_percentile_key(p) for p in service_time_metrics.keys()],
                "percentile_values": list(service_time_metrics.values()),
            })

    for op, results in service_time_per_op.items():
        _, ax = create_plot()
        legend_handles = []
        legend_labels = []

        for candidate in results:
            label = candidate["data_series"]
            series = ax.plot(candidate["percentiles"], candidate["percentile_values"], marker='.', label=label)
            legend_handles.append(series[0])
            legend_labels.append(label)

        ax.set_ylabel("Service Time [ms]")
        ax.set_xlabel("Percentile")
        ax.set_title("Service Time of %s" % op)
        ax.set_ylim(ymin=0)

        box = ax.get_position()
        ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])
        ax.legend(legend_handles, legend_labels, loc='center left', bbox_to_anchor=(1, 0.5))

        present(plt, "service_time_%s" % op)


def plot_throughput(raw_data, label_key):
    throughput_per_op = {}
    unit = ""

    for d in raw_data:
        data_series = data_series_name(d, label_key)
        for op_metrics in d["results"]["op_metrics"]:
            operation = op_metrics["operation"]
            throughput_metrics = op_metrics["throughput"]
            if operation not in throughput_per_op:
                throughput_per_op[operation] = []
            throughput_per_op[operation].append({
                "data_series": data_series,
                "max": throughput_metrics["max"],
                "median": throughput_metrics["median"],
                "min": throughput_metrics["min"],
                "unit": throughput_metrics["unit"]
            })

    for op, results in throughput_per_op.items():
        _, ax = create_plot()
        x_tick_labels = []
        throughput = []
        min_throughput = []
        max_throughput = []
        width = 0.35
        unit = ""

        for candidate in results:
            x_tick_labels.append(candidate["data_series"])
            cmin = candidate["min"]
            cmedian = candidate["median"]
            cmax = candidate["max"]
            # all units per op are the same but they can change across operations.
            unit = candidate["unit"]
            if cmin and cmedian and cmax:
                min_throughput.append(cmedian - cmin)
                throughput.append(cmedian)
                max_throughput.append(cmax - cmedian)
            else:
                min_throughput.append(0)
                throughput.append(0)
                max_throughput.append(0)

        indices = range(len(throughput))

        ax.bar(indices, throughput, width, yerr=[min_throughput, max_throughput])
        ax.set_xticks(indices)
        ax.set_xticklabels(x_tick_labels)
        ax.set_ylabel("Throughput [%s]" % unit)
        ax.set_title("Throughput of %s" % op)
        ax.set_ylim(ymin=0)

        present(plt, "throughput_%s" % op)


def plot_gc_times(raw_data, label_key):
    _, ax = create_plot()

    x_tick_labels = []
    old_gc_times = []
    young_gc_times = []
    width = 0.35

    for d in raw_data:
        data_series = data_series_name(d, label_key)

        x_tick_labels.append(data_series)

        old_gc_times.append(d["results"]["old_gc_time"])
        young_gc_times.append(d["results"]["young_gc_time"])

    indices = range(len(old_gc_times))

    old_bar = ax.bar(indices, old_gc_times, width)
    ax.set_xticks([x + width / 2 for x in indices])
    ax.set_xticklabels(x_tick_labels)
    ax.set_ylabel("Total Duration [ms]")
    ax.set_title("GC Times")

    indices = [x + width for x in indices]
    young_bar = ax.bar(indices, young_gc_times, width)

    box = ax.get_position()
    ax.set_position([box.x0, box.y0, box.width * 0.8, box.height])

    ax.legend([old_bar[0], young_bar[0]], ["Old GC", "Young GC"], loc='center left', bbox_to_anchor=(1, 0.5))
    ax.set_ylim(ymin=0)

    present(plt, "gc_times")


def plot(raw_data, label_key):
    plot_gc_times(raw_data, label_key)
    plot_throughput(raw_data, label_key)
    plot_service_time(raw_data, label_key)


def parse_args():
    parser = argparse.ArgumentParser(description="Turns test_execution.json files into graphs")

    parser.add_argument(
        "--label",
        help="defines which attribute to use for labelling data series (default: test-execution-timestamp).",
        # choices=["environment", "test-execution-timestamp", "user-tags", "test_procedure", "provision-config-instance"],
        default="test-execution-timestamp")

    parser.add_argument("path",
                        nargs="+",
                        help="Full path to one or more test_execution.json files")

    return parser.parse_args()


def main():
    args = parse_args()
    series = []

    for f in args.path:
        a_series = json.load(open(f, "rt"))
        if include(a_series):
            series.append(a_series)
    plot(series, args.label)


if __name__ == '__main__':
    main()
