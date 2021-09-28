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

import glob
import json
import logging
import uuid

from osbenchmark import workload, config, exceptions
from osbenchmark.utils import io, console

color_scheme_rgba = [
    # #00BFB3
    "rgba(0,191,179,1)",
    # #00A9E0
    "rgba(0,169,224,1)",
    # #F04E98
    "rgba(240,78,152,1)",
    # #FFCD00
    "rgba(255,205,0,1)",
    # #0076A8
    "rgba(0,118,168,1)",
    # #93C90E
    "rgba(147,201,14,1)",
    # #646464
    "rgba(100,100,100,1)",
]


def index_label(test_execution_config):
    if test_execution_config.label:
        return test_execution_config.label

    label = "%s-%s" % (test_execution_config.test_procedure, test_execution_config.provision_config_instance)
    if test_execution_config.plugins:
        label += "-%s" % test_execution_config.plugins.replace(":", "-").replace(",", "+")
    if test_execution_config.node_count > 1:
        label += " (%d nodes)" % test_execution_config.node_count
    return label


class BarCharts:
    UI_STATE_JSON = json.dumps(
        {
            "vis": {
                "colors": dict(
                    zip(["bare-oss", "bare-basic", "bare-trial-security", "docker-basic", "ear-basic"], color_scheme_rgba))
            }
        })

    @staticmethod
    # flavor's unused but we need the same signature used by the corresponding method in TimeSeriesCharts
    def format_title(environment, workload_name, flavor=None, os_license=None, suffix=None):
        title = f"{environment}-{workload_name}"

        if suffix:
            title += f"-{suffix}"

        return title

    @staticmethod
    def filter_string(environment, test_ex_config):
        if test_ex_config.name:
            return f"environment:\"{environment}\" AND active:true AND user-tags.name:\"{test_ex_config.name}\""
        else:
            return f"environment:\"{environment}\" AND active:true AND workload:\"{test_ex_config.workload}\""\
                   f" AND test_procedure:\"{test_ex_config.test_procedure}\""\
                   f" AND provision_config_instance:\"{test_ex_config.provision_config_instance}\""\
                   f" AND node-count:{test_ex_config.node_count}"

    @staticmethod
    def gc(title, environment, test_execution_config):
        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "filters"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "Total GC Duration [ms]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "Total GC Duration [ms]"
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "median",
                    "schema": "metric",
                    "params": {
                        "field": "value.single",
                        "percents": [
                            50
                        ],
                        "customLabel": "Total GC Duration [ms]"
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "filters",
                    "schema": "segment",
                    "params": {
                        "filters": [
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "query": "name:young_gc_time",
                                            "analyze_wildcard": True
                                        }
                                    }
                                },
                                "label": "Young GC"
                            },
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "query": "name:old_gc_time",
                                            "analyze_wildcard": True
                                        }
                                    }
                                },
                                "label": "Old GC"
                            }
                        ]
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "split",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term",
                        "row": False
                    }
                },
                {
                    "id": "4",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 5,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "benchmark-results-*",
            "query": {
                "query_string": {
                    "query": BarCharts.filter_string(environment, test_execution_config),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "gc",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def io(title, environment, test_execution_config):
        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "filters"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "[Bytes]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "[Bytes]"
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "sum",
                    "schema": "metric",
                    "params": {
                        "field": "value.single",
                        "customLabel": "[Bytes]"
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "filters",
                    "schema": "segment",
                    "params": {
                        "filters": [
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "analyze_wildcard": True,
                                            "query": "name:index_size"
                                        }
                                    }
                                },
                                "label": "Index size"
                            },
                            {
                                "input": {
                                    "query": {
                                        "query_string": {
                                            "analyze_wildcard": True,
                                            "query": "name:bytes_written"
                                        }
                                    }
                                },
                                "label": "Bytes written"
                            }
                        ]
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "split",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term",
                        "row": False
                    }
                },
                {
                    "id": "4",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 5,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "benchmark-results-*",
            "query": {
                "query_string": {
                    "query": BarCharts.filter_string(environment, test_execution_config),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "io",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def segment_memory(title, environment, test_execution_config):
        # don't generate segment memory charts for releases
        return None

    @staticmethod
    def query(environment, test_execution_config, q):
        metric = "service_time"
        title = BarCharts.format_title(
            environment,
            test_execution_config.workload,
            suffix="%s-%s-p99-%s" % (test_execution_config.label,
            q,
            metric))
        label = "Query Service Time [ms]"

        vis_state = {
            "title": title,
            "type": "histogram",
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "distribution-version: Ascending"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": label
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": label
                        },
                        "type": "value"
                    }
                ]
            },
            "aggs": [
                {
                    "id": "1",
                    "enabled": True,
                    "type": "median",
                    "schema": "metric",
                    "params": {
                        "field": "value.99_0",
                        "percents": [
                            50
                        ],
                        "customLabel": label
                    }
                },
                {
                    "id": "2",
                    "enabled": True,
                    "type": "terms",
                    "schema": "segment",
                    "params": {
                        "field": "distribution-version",
                        "size": 10,
                        "order": "asc",
                        "orderBy": "_term"
                    }
                },
                {
                    "id": "3",
                    "enabled": True,
                    "type": "terms",
                    "schema": "group",
                    "params": {
                        "field": "user-tags.setup",
                        "size": 10,
                        "order": "desc",
                        "orderBy": "_term"
                    }
                }
            ],
            "listeners": {}
        }

        search_source = {
            "index": "benchmark-results-*",
            "query": {
                "query_string": {
                    "query": "name:\"%s\" AND task:\"%s\" AND %s" % (
                        metric,
                        q,
                        BarCharts.filter_string(
                            environment,
                            test_execution_config)),
                    "analyze_wildcard": True
                }
            },
            "filter": []
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "query",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }

    @staticmethod
    def index(environment, test_execution_configs, title):
        filters = []
        for test_execution_config in test_execution_configs:
            label = index_label(test_execution_config)
            # the assumption is that we only have one bulk task
            for bulk_task in test_execution_config.bulk_tasks:
                filters.append({
                    "input": {
                        "query": {
                            "query_string": {
                                "analyze_wildcard": True,
                                "query": "task:\"%s\" AND %s" % (bulk_task, BarCharts.filter_string(environment, test_execution_config))
                            }
                        }
                    },
                    "label": label
                })

        vis_state = {
            "aggs": [
                {
                    "enabled": True,
                    "id": "1",
                    "params": {
                        "customLabel": "Median Indexing Throughput [docs/s]",
                        "field": "value.median",
                        "percents": [
                            50
                        ]
                    },
                    "schema": "metric",
                    "type": "median"
                },
                {
                    "enabled": True,
                    "id": "2",
                    "params": {
                        "field": "distribution-version",
                        "order": "asc",
                        "orderBy": "_term",
                        "size": 10
                    },
                    "schema": "segment",
                    "type": "terms"
                },
                {
                    "enabled": True,
                    "id": "3",
                    "params": {
                        "field": "user-tags.setup",
                        "order": "desc",
                        "orderBy": "_term",
                        "size": 10
                    },
                    "schema": "group",
                    "type": "terms"
                },
                {
                    "enabled": True,
                    "id": "4",
                    "params": {
                        "filters": filters
                    },
                    "schema": "split",
                    "type": "filters"
                }
            ],
            "listeners": {},
            "params": {
                "addLegend": True,
                "addTimeMarker": False,
                "addTooltip": True,
                "categoryAxes": [
                    {
                        "id": "CategoryAxis-1",
                        "labels": {
                            "show": True,
                            "truncate": 100
                        },
                        "position": "bottom",
                        "scale": {
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "distribution-version: Ascending"
                        },
                        "type": "category"
                    }
                ],
                "defaultYExtents": False,
                "drawLinesBetweenPoints": True,
                "grid": {
                    "categoryLines": False,
                    "style": {
                        "color": "#eee"
                    }
                },
                "interpolate": "linear",
                "legendPosition": "right",
                "radiusRatio": 9,
                "scale": "linear",
                "seriesParams": [
                    {
                        "data": {
                            "id": "1",
                            "label": "Median Indexing Throughput [docs/s]"
                        },
                        "drawLinesBetweenPoints": True,
                        "mode": "normal",
                        "show": "True",
                        "showCircles": True,
                        "type": "histogram",
                        "valueAxis": "ValueAxis-1"
                    }
                ],
                "setYExtents": False,
                "showCircles": True,
                "times": [],
                "valueAxes": [
                    {
                        "id": "ValueAxis-1",
                        "labels": {
                            "filter": False,
                            "rotate": 0,
                            "show": True,
                            "truncate": 100
                        },
                        "name": "LeftAxis-1",
                        "position": "left",
                        "scale": {
                            "mode": "normal",
                            "type": "linear"
                        },
                        "show": True,
                        "style": {},
                        "title": {
                            "text": "Median Indexing Throughput [docs/s]"
                        },
                        "type": "value"
                    }
                ],
                "row": True
            },
            "title": title,
            "type": "histogram"
        }

        search_source = {
            "index": "benchmark-results-*",
            "query": {
                "query_string": {
                    "analyze_wildcard": True,
                    "query": "environment:\"%s\" AND active:true AND name:\"throughput\"" % environment
                }
            },
            "filter": []
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": BarCharts.UI_STATE_JSON,
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps(search_source)
                }
            }
        }


class TimeSeriesCharts:
    @staticmethod
    def format_title(environment, workload_name, flavor=None, os_license=None, suffix=None):
        if flavor:
            title = [environment, flavor, str(workload_name)]
        elif os_license:
            title = [environment, os_license, str(workload_name)]
        elif flavor and os_license:
            raise exceptions.BenchmarkAssertionError(
                f"Specify either flavor [{flavor}] or license [{os_license}] but not both")
        else:
            title = [environment, str(workload_name)]
        if suffix:
            title.append(suffix)

        return "-".join(title)

    @staticmethod
    def filter_string(environment, test_ex_config):
        nightly_extra_filter = ""
        if test_ex_config.os_license:
            # Time series charts need to support different licenses and produce customized titles.
            nightly_extra_filter = f" AND user-tags.license:\"{test_ex_config.os_license}\""
        if test_ex_config.name:
            return f"environment:\"{environment}\" AND active:true AND user-tags.name:\"{test_ex_config.name}\"{nightly_extra_filter}"
        else:
            return f"environment:\"{environment}\" AND active:true AND workload:\"{test_ex_config.workload}\""\
                   f" AND test_procedure:\"{test_ex_config.test_procedure}\""\
                   f" AND provision_config_instance:\"{test_ex_config.provision_config_instance}\""\
                   f" AND node-count:{test_ex_config.node_count}"

    @staticmethod
    def gc(title, environment, test_ex_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "young_gc_time",
                                "label": "Young Gen GC time",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "old_gc_time",
                                "label": "Old Gen GC time",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "GC Times",
                        "value_template": "{{value}} ms",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, test_ex_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_ex_config.workload}\") "\
                            f"AND ((NOT _exists_:chart) OR chart:gc) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "axis_min": "0"
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "gc",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def merge_time(title, environment, test_execution_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "merge_time",
                                "label": "Cumulative merge time",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "merge_throttle_time",
                                "label": "Cumulative merge throttle time",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Merge Times",
                        "value_template": "{{value}} ms",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, test_execution_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_execution_config.workload}\") "
                                        f"AND ((NOT _exists_:chart) OR chart:merge_times) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "axis_min": "0"
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "merge_times",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def merge_count(title, environment, test_execution_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "merge_count",
                                "label": "Cumulative merge count",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Merge Count",
                        "value_template": "{{value}}",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, test_execution_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_execution_config.workload}\") "
                                        f"AND ((NOT _exists_:chart) OR chart:merge_count) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "axis_min": "0"
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "merge_count",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def io(title, environment, test_ex_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "bytes",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "sum",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "",
                        "split_filters": [
                            {
                                "filter": "name:index_size",
                                "label": "Index Size",
                                "color": "rgba(0,191,179,1)",
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "name:bytes_written",
                                "label": "Written",
                                "color": "rgba(254,209,10,1)",
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Disk IO",
                        "value_template": "{{value}}",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, test_ex_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_ex_config.workload}\") "\
                            f"AND ((NOT _exists_:chart) OR chart:io) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "axis_min": "0"
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "io",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def segment_memory(title, environment, test_ex_config):
        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "bytes",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.single"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": f"environment:{environment} AND workload:\"{test_ex_config.workload}\"",
                        "split_filters": [
                            {
                                "filter": "memory_segments",
                                "label": "Segments",
                                "color": color_scheme_rgba[0],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_doc_values",
                                "label": "Doc Values",
                                "color": color_scheme_rgba[1],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_terms",
                                "label": "Terms",
                                "color": color_scheme_rgba[2],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_norms",
                                "label": "Norms",
                                "color": color_scheme_rgba[3],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_points",
                                "label": "Points",
                                "color": color_scheme_rgba[4],
                                "id": str(uuid.uuid4())
                            },
                            {
                                "filter": "memory_stored_fields",
                                "label": "Stored Fields",
                                "color": color_scheme_rgba[5],
                                "id": str(uuid.uuid4())
                            }
                        ],
                        "label": "Segment Memory",
                        "value_template": "{{value}}",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": TimeSeriesCharts.filter_string(environment, test_ex_config),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_ex_config.workload}\") "
                                        f"AND ((NOT _exists_:chart) OR chart:segment_memory) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "show_grid": 1,
                "drop_last_bucket": 0,
                "axis_min": "0"
            },
            "aggs": []
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "segment_memory",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":{\"query\":\"*\",\"language\":\"lucene\"},\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def query(environment, test_ex_config, q):
        metric = "latency"
        title = TimeSeriesCharts.format_title(environment, test_ex_config.workload, os_license=test_ex_config.os_license,
                                              suffix="%s-%s-%s" % (test_ex_config.label, q, metric))

        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "id": str(uuid.uuid4()),
                "type": "timeseries",
                "series": [
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[0],
                        "split_mode": "everything",
                        "label": "50th percentile",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.50_0"
                            }
                        ],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.6",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[1],
                        "split_mode": "everything",
                        "label": "90th percentile",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.90_0"
                            }
                        ],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.4",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[2],
                        "split_mode": "everything",
                        "label": "99th percentile",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.99_0"
                            }
                        ],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.2",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    },
                    {
                        "id": str(uuid.uuid4()),
                        "color": color_scheme_rgba[3],
                        "split_mode": "everything",
                        "label": "100th percentile",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.100_0"
                            }
                        ],
                        "seperate_axis": 0,
                        "axis_position": "right",
                        "formatter": "number",
                        "chart_type": "line",
                        "line_width": 1,
                        "point_size": 1,
                        "fill": "0.1",
                        "stacked": "none",
                        "split_color_mode": "gradient",
                        "series_drop_last_bucket": 0,
                        "value_template": "{{value}} ms",
                    }
                ],
                "time_field": "test-execution-timestamp",
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "axis_position": "left",
                "axis_formatter": "number",
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "background_color_rules": [
                    {
                        "id": str(uuid.uuid4())
                    }
                ],
                "filter": "task:\"%s\" AND name:\"%s\" AND %s" % (q, metric, TimeSeriesCharts.filter_string(
                    environment, test_ex_config)),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{test_ex_config.workload}\") "
                                        f"AND ((NOT _exists_:chart) OR chart:query) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ]
            },
            "aggs": [],
            "listeners": {}
        }

        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "query",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }

    @staticmethod
    def index(environment, test_execution_configs, title):
        filters = []
        # any test_execution_config will do - they all belong to the same workload
        t = test_execution_configs[0].workload
        for idx, test_execution_config in enumerate(test_execution_configs):
            label = index_label(test_execution_config)
            for bulk_task in test_execution_config.bulk_tasks:
                filters.append(
                    {
                        "filter": "task:\"%s\" AND %s" % (bulk_task, TimeSeriesCharts.filter_string(environment, test_execution_config)),
                        "label": label,
                        "color": color_scheme_rgba[idx % len(color_scheme_rgba)],
                        "id": str(uuid.uuid4())
                    }
                )

        vis_state = {
            "title": title,
            "type": "metrics",
            "params": {
                "axis_formatter": "number",
                "axis_position": "left",
                "id": str(uuid.uuid4()),
                "index_pattern": "benchmark-results-*",
                "interval": "1d",
                "series": [
                    {
                        "axis_position": "left",
                        "chart_type": "line",
                        "color": "#68BC00",
                        "fill": "0",
                        "formatter": "number",
                        "id": str(uuid.uuid4()),
                        "line_width": "1",
                        "metrics": [
                            {
                                "id": str(uuid.uuid4()),
                                "type": "avg",
                                "field": "value.median"
                            }
                        ],
                        "point_size": "3",
                        "seperate_axis": 1,
                        "split_mode": "filters",
                        "stacked": "none",
                        "filter": "environment:\"%s\" AND workload:\"%s\"" % (environment, t),
                        "split_filters": filters,
                        "label": "Indexing Throughput",
                        "value_template": "{{value}} docs/s",
                        "steps": 0
                    }
                ],
                "show_legend": 1,
                "show_grid": 1,
                "drop_last_bucket": 0,
                "time_field": "test-execution-timestamp",
                "type": "timeseries",
                "filter": "environment:\"%s\" AND workload:\"%s\" AND name:\"throughput\" AND active:true" % (environment, t),
                "annotations": [
                    {
                        "fields": "message",
                        "template": "{{message}}",
                        "index_pattern": "benchmark-annotations",
                        "query_string": f"((NOT _exists_:workload) OR workload:\"{t}\") "
                                        f"AND ((NOT _exists_:chart) OR chart:indexing) "
                                        f"AND ((NOT _exists_:chart-name) OR chart-name:\"{title}\") AND environment:\"{environment}\"",
                        "id": str(uuid.uuid4()),
                        "color": "rgba(102,102,102,1)",
                        "time_field": "test-execution-timestamp",
                        "icon": "fa-tag",
                        "ignore_panel_filters": 1
                    }
                ],
                "axis_min": "0"
            },
            "aggs": [],
            "listeners": {}
        }
        return {
            "id": str(uuid.uuid4()),
            "type": "visualization",
            "attributes": {
                "title": title,
                "visState": json.dumps(vis_state),
                "uiStateJSON": "{}",
                "description": "index",
                "version": 1,
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": "{\"query\":\"*\",\"filter\":[]}"
                }
            }
        }


class TestExecutionConfigWorkload:
    def __init__(self, cfg, repository, name=None):
        self.repository = repository
        self.cached_workload = self.load_workload(cfg, name=name)

    def load_workload(self, cfg, name=None, params=None, excluded_tasks=None):
        if not params:
            params = {}
        # required in case a previous workload using a different repository has specified the revision
        if cfg.opts("workload", "repository.name", mandatory=False) != self.repository:
            cfg.add(config.Scope.applicationOverride, "workload", "repository.revision", None)
        # hack to make this work with multiple workloads (Benchmark core is usually not meant to be used this way)
        if name:
            cfg.add(config.Scope.applicationOverride, "workload", "repository.name", self.repository)
            cfg.add(config.Scope.applicationOverride, "workload", "workload.name", name)
        # another hack to ensure any workload-params in the test_execution config are used by Benchmark's workload loader
        cfg.add(config.Scope.applicationOverride, "workload", "params", params)
        if excluded_tasks:
            cfg.add(config.Scope.application, "workload", "exclude.tasks", excluded_tasks)
        return workload.load_workload(cfg)

    def get_workload(self, cfg, name=None, params=None, excluded_tasks=None):
        if params or excluded_tasks:
            return self.load_workload(cfg, name, params, excluded_tasks)
        # if no params specified, return the initially cached, (non-parametrized) workload
        return self.cached_workload


def generate_index_ops(chart_type, test_execution_configs, environment, logger):
    idx_test_execution_configs = list(filter(lambda c: "indexing" in c.charts, test_execution_configs))
    for test_execution_conf in idx_test_execution_configs:
        logger.debug("Gen index visualization for test_execution config with name:[%s] / label:[%s] / flavor: [%s] / license: [%s]",
                     test_execution_conf.name,
                     test_execution_conf.label,
                     test_execution_conf.flavor,
                     test_execution_conf.os_license)
    charts = []

    if idx_test_execution_configs:
        title = chart_type.format_title(
            environment,
            test_execution_configs[0].workload,
            flavor=test_execution_configs[0].flavor,
            suffix="indexing-throughput")
        charts = [chart_type.index(environment, idx_test_execution_configs, title)]
    return charts


def generate_queries(chart_type, test_execution_configs, environment):
    # output JSON structures
    structures = []

    for test_execution_config in test_execution_configs:
        if "query" in test_execution_config.charts:
            for q in test_execution_config.throttled_tasks:
                structures.append(chart_type.query(environment, test_execution_config, q))
    return structures


def generate_io(chart_type, test_execution_configs, environment):
    # output JSON structures
    structures = []
    for test_execution_config in test_execution_configs:
        if "io" in test_execution_config.charts:
            title = chart_type.format_title(environment, test_execution_config.workload, os_license=test_execution_config.os_license,
                                            suffix="%s-io" % test_execution_config.label)
            structures.append(chart_type.io(title, environment, test_execution_config))

    return structures


def generate_gc(chart_type, test_execution_configs, environment):
    structures = []
    for test_execution_config in test_execution_configs:
        if "gc" in test_execution_config.charts:
            title = chart_type.format_title(environment, test_execution_config.workload, os_license=test_execution_config.os_license,
                                            suffix="%s-gc" % test_execution_config.label)
            structures.append(chart_type.gc(title, environment, test_execution_config))

    return structures

def generate_merge_time(chart_type, test_execution_configs, environment):
    structures = []
    for test_execution_config in test_execution_configs:
        if "merge_times" in test_execution_config.charts:
            title = chart_type.format_title(environment, test_execution_config.workload, os_license=test_execution_config.os_license,
                                            suffix=f"{test_execution_config.label}-merge-times")
            structures.append(chart_type.merge_time(title, environment, test_execution_config))

    return structures

def generate_merge_count(chart_type, test_execution_configs, environment):
    structures = []
    for test_execution_config in test_execution_configs:
        if "merge_count" in test_execution_config.charts:
            title = chart_type.format_title(environment, test_execution_config.workload, os_license=test_execution_config.os_license,
                                            suffix=f"{test_execution_config.label}-merge-count")
            structures.append(chart_type.merge_count(title, environment, test_execution_config))

    return structures


def generate_segment_memory(chart_type, test_execution_configs, environment):
    structures = []
    for test_execution_config in test_execution_configs:
        if "segment_memory" in test_execution_config.charts:
            title = chart_type.format_title(environment, test_execution_config.workload, os_license=test_execution_config.os_license,
                                            suffix="%s-segment-memory" % test_execution_config.label)
            chart = chart_type.segment_memory(title, environment, test_execution_config)
            if chart:
                structures.append(chart)
    return structures


def generate_dashboard(chart_type, environment, workload, charts, flavor=None):
    panels = []

    width = 24
    height = 32

    row = 0
    col = 0

    for idx, chart in enumerate(charts):
        panelIndex = idx + 1
        # make index charts wider
        if chart["attributes"]["description"] == "index":
            chart_width = 2 * width
            # force one panel per row
            next_col = 0
        else:
            chart_width = width
            # two rows per panel
            next_col = (col + 1) % 2

        panel = {
            "id": chart["id"],
            "panelIndex": panelIndex,
            "gridData": {
                "x": (col * chart_width),
                "y": (row * height),
                "w": chart_width,
                "h": height,
                "i": str(panelIndex)
            },
            "type": "visualization",
            "version": "7.10.2"
        }
        panels.append(panel)
        col = next_col
        if col == 0:
            row += 1

    return {
        "id": str(uuid.uuid4()),
        "type": "dashboard",
        "attributes": {
            "title": chart_type.format_title(environment, workload.name, flavor=flavor),
            "hits": 0,
            "description": "",
            "panelsJSON": json.dumps(panels),
            "optionsJSON": "{\"darkTheme\":false}",
            "uiStateJSON": "{}",
            "version": 1,
            "timeRestore": False,
            "kibanaSavedObjectMeta": {
                "searchSourceJSON": json.dumps(
                    {
                        "filter": [
                            {
                                "query": {
                                    "query_string": {
                                        "analyze_wildcard": True,
                                        "query": "*"
                                    }
                                }
                            }
                        ],
                        "highlightAll": True,
                        "version": True
                    }
                )
            }
        }
    }


class TestExecutionConfig:
    def __init__(self, workload, cfg=None, flavor=None, os_license=None, \
        test_procedure=None, provision_config_instance=None, node_count=None,\
             charts=None):
        self.workload = workload
        if cfg:
            self.configuration = cfg
            self.configuration["flavor"] = flavor
            self.configuration["os_license"] = os_license
        else:
            self.configuration = {
                "charts": charts,
                "test_procedure": test_procedure,
                "provision-config-instance": provision_config_instance,
                "node-count": node_count
            }

    @property
    def name(self):
        return self.configuration.get("name")

    @property
    def flavor(self):
        return self.configuration.get("flavor")

    @property
    def os_license(self):
        return self.configuration.get("os_license")

    @property
    def label(self):
        return self.configuration.get("label")

    @property
    def charts(self):
        return self.configuration["charts"]

    @property
    def node_count(self):
        return self.configuration.get("node-count", 1)

    @property
    def test_procedure(self):
        return self.configuration["test_procedure"]

    @property
    def provision_config_instance(self):
        return self.configuration["provision-config-instance"]

    @property
    def plugins(self):
        return self.configuration.get("plugins", "")

    @property
    def bulk_tasks(self):
        task_names = []
        for task in self.workload.find_test_procedure_or_default(self.test_procedure).schedule:
            for sub_task in task:
                # We are looking for type bulk operations to add to indexing throughput chart.
                # For the observability workload, the index operation is of type raw-bulk, instead of type bulk.
                # Doing a lenient match to allow for that.
                if workload.OperationType.Bulk.to_hyphenated_string() in sub_task.operation.type:
                    if workload.OperationType.Bulk.to_hyphenated_string() != sub_task.operation.type:
                        console.info(f"Found [{sub_task.name}] of type [{sub_task.operation.type}] in "\
                                     f"[{self.test_procedure}], adding it to indexing dashboard.\n", flush=True)
                    task_names.append(sub_task.name)
        return task_names

    @property
    def throttled_tasks(self):
        task_names = []
        for task in self.workload.find_test_procedure_or_default(self.test_procedure).schedule:
            for sub_task in task:
                # We are assuming here that each task with a target throughput or target interval is interesting for latency charts.
                # We should refactor the chart generator to make this classification logic more flexible so the user can specify
                # which tasks / or types of operations should be used for which chart types.
                if "target-throughput" in sub_task.params or "target-interval" in sub_task.params:
                    task_names.append(sub_task.name)
        return task_names


def load_test_execution_configs(cfg, chart_type, chart_spec_path=None):
    def add_configs(test_execution_configs_per_lic, flavor_name="oss", lic="oss", workload_name=None):
        configs_per_lic = []
        for test_execution_config in test_execution_configs_per_lic:
            excluded_tasks = None
            if "exclude-tasks" in test_execution_config:
                excluded_tasks = test_execution_config.get("exclude-tasks").split(",")
            configs_per_lic.append(
                TestExecutionConfig(workload=test_execution_config_workload.get_workload(cfg, name=workload_name,
                                                             params=test_execution_config.get("workload-params", {}),
                                                             excluded_tasks=excluded_tasks),
                           cfg=test_execution_config,
                           flavor=flavor_name,
                           os_license=lic)
            )
        return configs_per_lic

    def add_test_execution_configs(license_configs, flavor_name, workload_name):
        if chart_type == BarCharts:
            # Only one license config, "basic", is present in bar charts
            _lic_conf = [license_config["configurations"] for license_config in license_configs if license_config["name"] == "basic"]
            if _lic_conf:
                test_execution_configs_per_workload.extend(add_configs(_lic_conf[0], workload_name=workload_name))
        else:
            for lic_config in license_configs:
                test_execution_configs_per_workload.extend(add_configs(lic_config["configurations"],
                                                          flavor_name,
                                                          lic_config["name"],
                                                          workload_name))

    test_execution_configs = {"oss": [], "default": []}
    if chart_type == BarCharts:
        test_execution_configs = []

    for _workload_file in glob.glob(io.normalize_path(chart_spec_path)):
        with open(_workload_file, mode="rt", encoding="utf-8") as f:
            for item in json.load(f):
                _workload_repository = item.get("workload-repository", "default")
                test_execution_config_workload = TestExecutionConfigWorkload(cfg, _workload_repository, name=item["workload"])
                for flavor in item["flavors"]:
                    test_execution_configs_per_workload = []
                    _flavor_name = flavor["name"]
                    _workload_name = item["workload"]
                    add_test_execution_configs(flavor["licenses"], _flavor_name, _workload_name)

                    if test_execution_configs_per_workload:
                        if chart_type == BarCharts:
                            test_execution_configs.append(test_execution_configs_per_workload)
                        else:
                            test_execution_configs[_flavor_name].append(test_execution_configs_per_workload)
    return test_execution_configs


def gen_charts_per_workload_configs(test_execution_configs, chart_type, env, flavor=None, logger=None):
    charts = generate_index_ops(chart_type, test_execution_configs, env, logger) + \
             generate_io(chart_type, test_execution_configs, env) + \
             generate_gc(chart_type, test_execution_configs, env) + \
             generate_merge_time(chart_type, test_execution_configs, env) + \
             generate_merge_count(chart_type, test_execution_configs, env) + \
             generate_segment_memory(chart_type, test_execution_configs, env) + \
             generate_queries(chart_type, test_execution_configs, env)

    dashboard = generate_dashboard(chart_type, env, test_execution_configs[0].workload, charts, flavor)

    return charts, dashboard


def gen_charts_per_workload(test_execution_configs, chart_type, env, flavor=None, logger=None):
    structures = []
    for test_execution_configs_per_workload in test_execution_configs:
        charts, dashboard = gen_charts_per_workload_configs(test_execution_configs_per_workload, chart_type, env, flavor, logger)
        structures.extend(charts)
        structures.append(dashboard)

    return structures


def gen_charts_from_workload_combinations(test_execution_configs, chart_type, env, logger):
    structures = []
    for flavor, test_execution_configs_per_flavor in test_execution_configs.items():
        for test_execution_configs_per_workload in test_execution_configs_per_flavor:
            logger.debug("Generating charts for test_execution_configs with name:[%s]/flavor:[%s]",
                         test_execution_configs_per_workload[0].name, flavor)
            charts, dashboard = gen_charts_per_workload_configs(test_execution_configs_per_workload, chart_type, env, flavor, logger)

            structures.extend(charts)
            structures.append(dashboard)

    return structures


def generate(cfg):
    logger = logging.getLogger(__name__)

    chart_spec_path = cfg.opts("generator", "chart.spec.path")
    if cfg.opts("generator", "chart.type") == "time-series":
        chart_type = TimeSeriesCharts
    else:
        chart_type = BarCharts

    console.info("Loading workload data...", flush=True)
    test_execution_configs = load_test_execution_configs(cfg, chart_type, chart_spec_path)
    env = cfg.opts("system", "env.name")

    structures = []
    console.info("Generating charts...", flush=True)

    if chart_type == BarCharts:
        # bar charts are flavor agnostic and split results based on a separate `user.setup` field
        structures = gen_charts_per_workload(test_execution_configs, chart_type, env, logger=logger)
    elif chart_type == TimeSeriesCharts:
        structures = gen_charts_from_workload_combinations(test_execution_configs, chart_type, env, logger)

    output_path = cfg.opts("generator", "output.path")
    if output_path:
        with open(io.normalize_path(output_path), mode="wt", encoding="utf-8") as f:
            for record in structures:
                print(json.dumps(record), file=f)
    else:
        for record in structures:
            print(json.dumps(record))
