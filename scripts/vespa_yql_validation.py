#!/usr/bin/env python3
"""
Validate all Vespa YQL translations against a live Vespa instance.

Tests every query pattern used across the 5 OSB workloads (big5, http_logs,
nyc_taxis, pmc, so) by converting OpenSearch DSL to YQL via the OSB helper
functions, then executing the resulting YQL against the Vespa cluster.

Usage:
    python scripts/vespa_yql_validation.py [--url http://host:port]
"""

import argparse
import json
import logging
import sys
import traceback
from typing import Any, Dict, List, Optional, Tuple

from osbenchmark.database.clients.vespa.helpers import (
    build_grouping_clause,
    convert_aggregation,
    convert_to_yql,
)

# Suppress noisy pyvespa / httpr logging
logging.getLogger("httpr").setLevel(logging.WARNING)
logging.getLogger("vespa").setLevel(logging.WARNING)

VESPA_URL = "http://10.0.142.54:8080"

# ---------------------------------------------------------------------------
# Test definitions per workload
# ---------------------------------------------------------------------------
# Each entry: (operation_name, body, document_type)
# document_type mirrors what OSB passes: the OpenSearch index name.


def big5_tests() -> List[Tuple[str, Dict, str]]:
    """big5 workload queries — document type 'big5'."""
    doc = "big5"
    return [
        # -- match_all --
        ("match-all", {
            "query": {"match_all": {}}
        }, doc),

        # -- term --
        ("term", {
            "query": {
                "term": {
                    "log.file.path": {
                        "value": "/var/log/messages/birdknight"
                    }
                }
            }
        }, doc),

        # -- range (date) --
        ("range", {
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "2023-01-01T00:00:00",
                        "lt": "2023-01-03T00:00:00"
                    }
                }
            }
        }, doc),

        # -- range (numeric) --
        ("range-numeric", {
            "query": {
                "range": {
                    "metrics.size": {
                        "gte": 20,
                        "lte": 200
                    }
                }
            }
        }, doc),

        # -- bool: keyword-in-range --
        ("keyword-in-range", {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "2023-01-01T00:00:00", "lt": "2023-01-03T00:00:00"}}},
                        {"match": {"process.name": "kernel"}}
                    ]
                }
            }
        }, doc),

        # -- date_histogram hourly --
        ("date_histogram_hourly_agg", {
            "size": 0,
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "hour"
                    }
                }
            }
        }, doc),

        # -- date_histogram hourly with filter --
        ("date_histogram_hourly_with_filter_agg", {
            "size": 0,
            "query": {"term": {"process.name": "systemd"}},
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "hour"
                    }
                }
            }
        }, doc),

        # -- date_histogram minute with range filter --
        ("date_histogram_minute_agg", {
            "size": 0,
            "query": {
                "range": {
                    "@timestamp": {
                        "gte": "2023-01-01T00:00:00",
                        "lt": "2023-01-03T00:00:00"
                    }
                }
            },
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "minute"
                    }
                }
            }
        }, doc),

        # -- desc sort timestamp --
        ("desc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "desc"}]
        }, doc),

        # -- desc sort with search_after --
        ("desc_sort_with_after_timestamp", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "desc"}],
            "search_after": ["2023-01-01T23:59:58.000Z"]
        }, doc),

        # -- asc sort timestamp --
        ("asc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "asc"}]
        }, doc),

        # -- asc sort with search_after --
        ("asc_sort_with_after_timestamp", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "asc"}],
            "search_after": ["2023-01-01T23:59:58.000Z"]
        }, doc),

        # -- sort with match (can_match shortcut) --
        ("desc_sort_timestamp_can_match_shortcut", {
            "track_total_hits": False,
            "query": {"match": {"process.name": "kernel"}},
            "sort": [{"@timestamp": "desc"}]
        }, doc),

        # -- sort keyword --
        ("sort_keyword_can_match_shortcut", {
            "track_total_hits": False,
            "query": {"match": {"process.name": "kernel"}},
            "sort": [{"meta.file": "asc"}]
        }, doc),

        # -- sort numeric desc --
        ("sort_numeric_desc", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"metrics.size": "desc"}]
        }, doc),

        # -- sort numeric asc --
        ("sort_numeric_asc", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"metrics.size": "asc"}]
        }, doc),

        # -- sort numeric desc with match --
        ("sort_numeric_desc_with_match", {
            "track_total_hits": False,
            "query": {"match": {"log.file.path": "/var/log/messages/solarshark"}},
            "sort": [{"metrics.size": "desc"}]
        }, doc),

        # -- nested terms + significant_terms agg --
        ("terms-significant-1", {
            "track_total_hits": False,
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-01T00:00:00", "lt": "2023-01-03T00:00:00"}}
            },
            "aggs": {
                "terms": {
                    "terms": {"field": "aws.cloudwatch.log_stream", "size": 10},
                    "aggs": {
                        "significant_ips": {"significant_terms": {"field": "process.name"}}
                    }
                }
            }
        }, doc),

        # -- bool must conjunction --
        ("range_field_conjunction_big_range_big_term_query", {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"process.name": "systemd"}},
                        {"range": {"metrics.size": {"gte": 1, "lte": 100}}}
                    ]
                }
            }
        }, doc),

        # -- bool should disjunction --
        ("range_field_disjunction_big_range_small_term_query", {
            "query": {
                "bool": {
                    "should": [
                        {"term": {"aws.cloudwatch.log_stream": "indigodagger"}},
                        {"range": {"metrics.size": {"gte": 1, "lte": 100}}}
                    ]
                }
            }
        }, doc),

        # -- range-auto-date-histo (nested range + auto_date_histogram) --
        ("range-auto-date-histo", {
            "size": 0,
            "aggs": {
                "tmax": {
                    "range": {
                        "field": "metrics.size",
                        "ranges": [
                            {"to": -10},
                            {"from": -10, "to": 10},
                            {"from": 10, "to": 100},
                            {"from": 100, "to": 1000},
                            {"from": 1000, "to": 2000},
                            {"from": 2000}
                        ]
                    },
                    "aggs": {
                        "date": {
                            "auto_date_histogram": {
                                "field": "@timestamp",
                                "buckets": 20
                            }
                        }
                    }
                }
            }
        }, doc),

        # -- range-with-metrics (nested range + sum/min/avg/max/stats) --
        ("range-with-metrics", {
            "size": 0,
            "aggs": {
                "tmax": {
                    "range": {
                        "field": "metrics.size",
                        "ranges": [
                            {"to": -10},
                            {"from": -10, "to": 10},
                            {"from": 10, "to": 100},
                            {"from": 100, "to": 1000},
                            {"from": 1000, "to": 2000},
                            {"from": 2000}
                        ]
                    },
                    "aggs": {
                        "tsum": {"sum": {"field": "metrics.size"}},
                        "tmin": {"min": {"field": "metrics.tmin"}},
                        "tavg": {"avg": {"field": "metrics.size"}},
                        "tmax": {"max": {"field": "metrics.size"}},
                        "tstats": {"stats": {"field": "metrics.size"}}
                    }
                }
            }
        }, doc),

        # -- range-auto-date-histo-with-metrics (3-level nesting) --
        ("range-auto-date-histo-with-metrics", {
            "size": 0,
            "aggs": {
                "tmax": {
                    "range": {
                        "field": "metrics.size",
                        "ranges": [
                            {"to": 100},
                            {"from": 100, "to": 1000},
                            {"from": 1000, "to": 2000},
                            {"from": 2000}
                        ]
                    },
                    "aggs": {
                        "date": {
                            "auto_date_histogram": {
                                "field": "@timestamp",
                                "buckets": 10
                            },
                            "aggs": {
                                "tmin": {"min": {"field": "metrics.tmin"}},
                                "tavg": {"avg": {"field": "metrics.size"}},
                                "tmax": {"max": {"field": "metrics.size"}}
                            }
                        }
                    }
                }
            }
        }, doc),

        # -- range-agg-1 (standalone range agg) --
        ("range-agg-1", {
            "size": 0,
            "aggs": {
                "tmax": {
                    "range": {
                        "field": "metrics.size",
                        "ranges": [
                            {"to": -10},
                            {"from": -10, "to": 10},
                            {"from": 10, "to": 100},
                            {"from": 100, "to": 1000},
                            {"from": 1000, "to": 2000},
                            {"from": 2000}
                        ]
                    }
                }
            }
        }, doc),

        # -- multi_terms-keyword --
        ("multi_terms-keyword", {
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-05T00:00:00", "lt": "2023-01-05T05:00:00"}}
            },
            "aggs": {
                "important_terms": {
                    "multi_terms": {
                        "terms": [
                            {"field": "process.name"},
                            {"field": "cloud.region"}
                        ]
                    }
                }
            }
        }, doc),

        # -- composite-terms --
        ("composite-terms", {
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-02T00:00:00", "lt": "2023-01-02T10:00:00"}}
            },
            "aggs": {
                "logs": {
                    "composite": {
                        "sources": [
                            {"process_name": {"terms": {"field": "process.name", "order": "desc"}}},
                            {"cloud_region": {"terms": {"field": "cloud.region", "order": "asc"}}}
                        ]
                    }
                }
            }
        }, doc),

        # -- composite_terms-keyword (3 sources) --
        ("composite_terms-keyword", {
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-02T00:00:00", "lt": "2023-01-02T10:00:00"}}
            },
            "aggs": {
                "logs": {
                    "composite": {
                        "sources": [
                            {"process_name": {"terms": {"field": "process.name", "order": "desc"}}},
                            {"cloud_region": {"terms": {"field": "cloud.region", "order": "asc"}}},
                            {"cloudstream": {"terms": {"field": "aws.cloudwatch.log_stream", "order": "asc"}}}
                        ]
                    }
                }
            }
        }, doc),

        # -- composite-date_histogram-daily --
        ("composite-date_histogram-daily", {
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "2022-12-30T00:00:00", "lt": "2023-01-07T12:00:00"}}
            },
            "aggs": {
                "logs": {
                    "composite": {
                        "sources": [
                            {"date": {"date_histogram": {"field": "@timestamp", "calendar_interval": "day"}}}
                        ]
                    }
                }
            }
        }, doc),

        # -- keyword-terms --
        ("keyword-terms", {
            "size": 0,
            "aggs": {
                "station": {
                    "terms": {"field": "aws.cloudwatch.log_stream", "size": 500}
                }
            }
        }, doc),

        # -- keyword-terms-low-cardinality --
        ("keyword-terms-low-cardinality", {
            "size": 0,
            "aggs": {
                "country": {
                    "terms": {"field": "aws.cloudwatch.log_stream", "size": 50}
                }
            }
        }, doc),

        # -- cardinality-agg-low --
        ("cardinality-agg-low", {
            "size": 0,
            "aggs": {
                "region": {
                    "cardinality": {"field": "cloud.region"}
                }
            }
        }, doc),

        # -- cardinality-agg-high --
        ("cardinality-agg-high", {
            "size": 0,
            "aggs": {
                "agent": {
                    "cardinality": {"field": "agent.name"}
                }
            }
        }, doc),

        # -- query-string-on-message --
        ("query-string-on-message", {
            "query": {
                "query_string": {"query": "message: monkey jackal bear"}
            }
        }, doc),

        # -- query-string-on-message-filtered --
        ("query-string-on-message-filtered", {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "2023-01-03T00:00:00", "lt": "2023-01-03T10:00:00"}}},
                        {"query_string": {"query": "message: monkey jackal bear"}}
                    ]
                }
            }
        }, doc),

        # -- query-string-on-message-filtered-sorted-num --
        ("query-string-on-message-filtered-sorted-num", {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "2023-01-03T00:00:00", "lt": "2023-01-03T10:00:00"}}},
                        {"query_string": {"query": "message: monkey jackal bear"}}
                    ]
                }
            },
            "sort": [{"@timestamp": {"order": "asc"}}]
        }, doc),

        # -- range_with_asc_sort --
        ("range_with_asc_sort", {
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-01T00:00:00", "lte": "2023-01-13T00:00:00"}}
            },
            "sort": [{"@timestamp": "asc"}]
        }, doc),

        # -- range_with_desc_sort --
        ("range_with_desc_sort", {
            "query": {
                "range": {"@timestamp": {"gte": "2023-01-01T00:00:00", "lte": "2023-01-13T00:00:00"}}
            },
            "sort": [{"@timestamp": "desc"}]
        }, doc),
    ]


def http_logs_tests() -> List[Tuple[str, Dict, str]]:
    """http_logs workload queries -- uses hyphenated document type 'logs-181998'."""
    doc = "logs-181998"
    return [
        # -- match_all --
        ("match-all", {
            "query": {"match_all": {}}
        }, doc),

        # -- term --
        ("term", {
            "query": {
                "term": {
                    "request.raw": {"value": "GET / HTTP/1.0"}
                }
            }
        }, doc),

        # -- multi-term-filter (bool must + filter) --
        ("multi-term-filter", {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"request.raw": {"value": "GET / HTTP/1.0"}}}
                    ],
                    "filter": [
                        {"term": {"status": 200}}
                    ]
                }
            }
        }, doc),

        # -- range (date) --
        ("200s-in-range", {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "1998-05-01T00:00:00Z", "lt": "1998-05-02T00:00:00Z"}}},
                        {"match": {"status": "200"}}
                    ]
                }
            }
        }, doc),

        # -- 400s-in-range --
        ("400s-in-range", {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"@timestamp": {"gte": "1998-05-01T00:00:00Z", "lt": "1998-05-02T00:00:00Z"}}},
                        {"match": {"status": "400"}}
                    ]
                }
            }
        }, doc),

        # -- hourly_agg (date_histogram) --
        ("hourly_agg", {
            "size": 0,
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "hour"
                    }
                }
            }
        }, doc),

        # -- hourly_agg_with_filter --
        ("hourly_agg_with_filter", {
            "query": {"term": {"status": 200}},
            "size": 0,
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "hour"
                    }
                }
            }
        }, doc),

        # -- hourly_agg_with_filter_and_metrics (date_histogram + stats) --
        ("hourly_agg_with_filter_and_metrics", {
            "query": {"term": {"status": 200}},
            "size": 0,
            "aggs": {
                "by_hour": {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval": "hour"
                    }
                },
                "size_stats": {
                    "stats": {"field": "size"}
                }
            }
        }, doc),

        # -- multi_term_agg --
        ("multi_term_agg", {
            "size": 0,
            "query": {
                "range": {"@timestamp": {"gte": "1998-05-03T00:00:00Z", "lt": "1998-05-07T00:00:00Z"}}
            },
            "aggs": {
                "mterms": {
                    "multi_terms": {
                        "terms": [
                            {"field": "clientip"},
                            {"field": "status"},
                            {"field": "size"}
                        ]
                    }
                }
            }
        }, doc),

        # -- desc_sort_size --
        ("desc_sort_size", {
            "query": {"match_all": {}},
            "sort": [{"size": "desc"}]
        }, doc),

        # -- asc_sort_size --
        ("asc_sort_size", {
            "query": {"match_all": {}},
            "sort": [{"size": "asc"}]
        }, doc),

        # -- range_size --
        ("range_size", {
            "query": {"range": {"size": {"gte": 20, "lte": 200}}}
        }, doc),

        # -- desc_sort_timestamp --
        ("desc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "desc"}]
        }, doc),

        # -- desc_sort_with_after_timestamp (search_after) --
        ("desc_sort_with_after_timestamp", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "desc"}],
            "search_after": ["1998-06-10"]
        }, doc),

        # -- asc_sort_timestamp --
        ("asc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "asc"}]
        }, doc),

        # -- asc_sort_with_after_timestamp (search_after) --
        ("asc_sort_with_after_timestamp", {
            "track_total_hits": False,
            "query": {"match_all": {}},
            "sort": [{"@timestamp": "asc"}],
            "search_after": ["1998-06-10"]
        }, doc),

        # -- range_with_desc_sort --
        ("range_with_desc_sort", {
            "size": 100,
            "query": {"range": {"size": {"gte": 10, "lte": 200}}},
            "sort": [{"size": "desc"}]
        }, doc),

        # -- range_with_asc_sort --
        ("range_with_asc_sort", {
            "size": 100,
            "query": {"range": {"size": {"gte": 10, "lte": 200}}},
            "sort": [{"size": "asc"}]
        }, doc),
    ]


def nyc_taxis_tests() -> List[Tuple[str, Dict, str]]:
    """nyc_taxis workload queries."""
    doc = "nyc_taxis"
    return [
        # -- match_all --
        ("match-all", {
            "query": {"match_all": {}}
        }, doc),

        # -- range (numeric) --
        ("range", {
            "query": {"range": {"total_amount": {"gte": 5, "lt": 15}}}
        }, doc),

        # -- histogram + stats (distance_amount_agg) --
        ("distance_amount_agg", {
            "size": 0,
            "query": {
                "bool": {
                    "filter": {
                        "range": {"trip_distance": {"lt": 50, "gte": 0}}
                    }
                }
            },
            "aggs": {
                "distance_histo": {
                    "histogram": {"field": "trip_distance", "interval": 1},
                    "aggs": {
                        "total_amount_stats": {"stats": {"field": "total_amount"}}
                    }
                }
            }
        }, doc),

        # -- auto_date_histogram --
        ("autohisto_agg", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lte": "2015-01-21 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "auto_date_histogram": {
                        "field": "dropoff_datetime",
                        "buckets": 20
                    }
                }
            }
        }, doc),

        # -- date_histogram_agg (daily) --
        ("date_histogram_agg", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lte": "2015-01-21 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "calendar_interval": "day"
                    }
                }
            }
        }, doc),

        # -- date_histogram_calendar_interval (monthly) --
        ("date_histogram_calendar_interval", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lt": "2016-01-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "calendar_interval": "month"
                    }
                }
            }
        }, doc),

        # -- date_histogram_fixed_interval --
        ("date_histogram_fixed_interval", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lt": "2016-01-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "fixed_interval": "60d"
                    }
                }
            }
        }, doc),

        # -- date_histogram_fixed_interval_with_metrics --
        ("date_histogram_fixed_interval_with_metrics", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lt": "2016-01-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "date_histogram": {
                        "field": "dropoff_datetime",
                        "fixed_interval": "60d"
                    },
                    "aggs": {
                        "total_amount": {"stats": {"field": "total_amount"}},
                        "tip_amount": {"stats": {"field": "tip_amount"}},
                        "trip_distance": {"stats": {"field": "trip_distance"}}
                    }
                }
            }
        }, doc),

        # -- auto_date_histogram --
        ("auto_date_histogram", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lt": "2016-01-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "auto_date_histogram": {
                        "field": "dropoff_datetime",
                        "buckets": "12"
                    }
                }
            }
        }, doc),

        # -- auto_date_histogram_with_metrics --
        ("auto_date_histogram_with_metrics", {
            "size": 0,
            "query": {
                "range": {
                    "dropoff_datetime": {
                        "gte": "2015-01-01 00:00:00",
                        "lt": "2016-01-01 00:00:00"
                    }
                }
            },
            "aggs": {
                "dropoffs_over_time": {
                    "auto_date_histogram": {
                        "field": "dropoff_datetime",
                        "buckets": "12"
                    },
                    "aggs": {
                        "total_amount": {"stats": {"field": "total_amount"}},
                        "tip_amount": {"stats": {"field": "tip_amount"}},
                        "trip_distance": {"stats": {"field": "trip_distance"}}
                    }
                }
            }
        }, doc),

        # -- desc_sort_tip_amount --
        ("desc_sort_tip_amount", {
            "query": {"match_all": {}},
            "sort": [{"tip_amount": "desc"}]
        }, doc),

        # -- asc_sort_tip_amount --
        ("asc_sort_tip_amount", {
            "query": {"match_all": {}},
            "sort": [{"tip_amount": "asc"}]
        }, doc),

        # -- desc_sort_passenger_count --
        ("desc_sort_passenger_count", {
            "query": {"match_all": {}},
            "sort": [{"passenger_count": "desc"}]
        }, doc),

        # -- asc_sort_passenger_count --
        ("asc_sort_passenger_count", {
            "query": {"match_all": {}},
            "sort": [{"passenger_count": "asc"}]
        }, doc),
    ]


def pmc_tests() -> List[Tuple[str, Dict, str]]:
    """pmc workload queries."""
    doc = "pmc"
    return [
        # -- match_all --
        ("match-all", {
            "query": {"match_all": {}}
        }, doc),

        # -- term --
        ("term", {
            "query": {"term": {"body": "physician"}}
        }, doc),

        # -- match_phrase --
        ("phrase", {
            "query": {"match_phrase": {"body": "newspaper coverage"}}
        }, doc),

        # -- articles_monthly_agg (date_histogram monthly) --
        ("articles_monthly_agg_uncached", {
            "size": 0,
            "aggs": {
                "articles_over_time": {
                    "date_histogram": {
                        "field": "timestamp",
                        "calendar_interval": "month"
                    }
                }
            }
        }, doc),

        # -- asc_sort_timestamp --
        ("asc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"timestamp": "asc"}]
        }, doc),

        # -- desc_sort_timestamp --
        ("desc_sort_timestamp", {
            "query": {"match_all": {}},
            "sort": [{"timestamp": "desc"}]
        }, doc),

        # -- asc_sort_pmid --
        ("asc_sort_pmid", {
            "query": {"match_all": {}},
            "sort": [{"pmid": "asc"}]
        }, doc),

        # -- desc_sort_pmid --
        ("desc_sort_pmid", {
            "query": {"match_all": {}},
            "sort": [{"pmid": "desc"}]
        }, doc),
    ]


def so_tests() -> List[Tuple[str, Dict, str]]:
    """so (StackOverflow) workload queries.

    The so workload is indexing-only — it has no search operations defined.
    We create representative queries based on its schema fields:
    user (keyword), creationDate (date), title (text), tags (keyword),
    body (text), type (keyword), questionId (keyword).
    """
    doc = "so"
    return [
        # -- match_all --
        ("match-all", {
            "query": {"match_all": {}}
        }, doc),

        # -- term on keyword --
        ("term-keyword", {
            "query": {"term": {"type": "question"}}
        }, doc),

        # -- match on text field --
        ("match-body", {
            "query": {"match": {"body": "python"}}
        }, doc),

        # -- match_phrase on body --
        ("phrase-body", {
            "query": {"match_phrase": {"body": "stack overflow"}}
        }, doc),

        # -- range on date --
        ("range-creationDate", {
            "query": {
                "range": {
                    "creationDate": {
                        "gte": "2015-01-01T00:00:00",
                        "lt": "2015-02-01T00:00:00"
                    }
                }
            }
        }, doc),

        # -- bool (term + range) --
        ("bool-type-and-date", {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"type": "question"}},
                        {"range": {"creationDate": {"gte": "2015-01-01T00:00:00", "lt": "2015-02-01T00:00:00"}}}
                    ]
                }
            }
        }, doc),

        # -- terms agg on keyword --
        ("terms-agg-tags", {
            "size": 0,
            "aggs": {
                "top_tags": {"terms": {"field": "tags", "size": 20}}
            }
        }, doc),

        # -- date_histogram on creationDate --
        ("date-histogram-monthly", {
            "size": 0,
            "aggs": {
                "posts_over_time": {
                    "date_histogram": {
                        "field": "creationDate",
                        "calendar_interval": "month"
                    }
                }
            }
        }, doc),

        # -- sort by creationDate desc --
        ("sort-creationDate-desc", {
            "query": {"match_all": {}},
            "sort": [{"creationDate": "desc"}]
        }, doc),
    ]


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

def run_tests(url: str) -> Tuple[int, int, int]:
    """Execute all tests and print results.

    Returns (total, passed, failed).
    """
    from vespa.application import Vespa  # imported here so --help works without pyvespa

    app = Vespa(url=url)

    all_tests = [
        ("big5", big5_tests()),
        ("http_logs", http_logs_tests()),
        ("nyc_taxis", nyc_taxis_tests()),
        ("pmc", pmc_tests()),
        ("so", so_tests()),
    ]

    total = 0
    passed = 0
    failed = 0

    with app.syncio(compress=False) as sync:
        for workload_name, tests in all_tests:
            print(f"\n{'='*60}")
            print(f"  Workload: {workload_name}  ({len(tests)} queries)")
            print(f"{'='*60}")

            for op_name, body, document_type in tests:
                total += 1
                try:
                    # Step 1: Convert OpenSearch DSL to YQL
                    yql, query_params = convert_to_yql(body, document_type)

                    # Step 2: Build the full params dict for Vespa
                    params = {"yql": yql}
                    params.update(query_params)

                    # Pass size=0 as hits=0 for agg-only queries
                    size = body.get("size", 10)
                    params["hits"] = size

                    # Add timeout
                    params.setdefault("timeout", "30s")

                    # Step 3: Execute against Vespa
                    result = sync.query(body=params)
                    response = result.json

                    # Step 4: Check for errors
                    errors = response.get("root", {}).get("errors", [])
                    if errors:
                        error_msgs = "; ".join(
                            e.get("message", str(e)) for e in errors
                        )
                        print(f"  [{workload_name}] {op_name}: FAIL: {error_msgs}")
                        print(f"           YQL: {yql}")
                        failed += 1
                    else:
                        total_count = response.get("root", {}).get("fields", {}).get("totalCount", "?")
                        print(f"  [{workload_name}] {op_name}: OK (totalCount={total_count})")
                        passed += 1

                except Exception as exc:
                    print(f"  [{workload_name}] {op_name}: FAIL: {exc}")
                    # Print YQL if we got that far
                    try:
                        yql_debug, _ = convert_to_yql(body, document_type)
                        print(f"           YQL: {yql_debug}")
                    except Exception:
                        pass
                    traceback.print_exc(limit=2)
                    failed += 1

    return total, passed, failed


def main():
    parser = argparse.ArgumentParser(
        description="Validate Vespa YQL translations against a live instance"
    )
    parser.add_argument(
        "--url",
        default=VESPA_URL,
        help=f"Vespa endpoint URL (default: {VESPA_URL})",
    )
    args = parser.parse_args()

    print(f"Vespa YQL Validation — target: {args.url}")
    print(f"Testing convert_to_yql + build_grouping_clause against live Vespa\n")

    total, passed, failed = run_tests(args.url)

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed}/{total} passed, {failed} failed")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)


if __name__ == "__main__":
    main()
