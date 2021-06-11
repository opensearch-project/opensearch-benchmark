import random
import os
import csv


class QueryParamSource:
    # We need to stick to the param source API
    # noinspection PyUnusedLocal
    def __init__(self, track, params, **kwargs):
        self._params = params
        self.infinite = True
        # here we read the queries data file into arrays which we'll then later use randomly.
        self.tags = []
        self.dates = []
        # be predictably random. The seed has been chosen by a fair dice roll. ;)
        random.seed(4)
        cwd = os.path.dirname(__file__)
        with open(os.path.join(cwd, "queries.csv"), "r") as ins:
            csvreader = csv.reader(ins)
            for row in csvreader:
                self.tags.append(row[0])
                self.dates.append(row[1])

    # We need to stick to the param source API
    # noinspection PyUnusedLocal
    def partition(self, partition_index, total_partitions):
        return self


class SortedTermQueryParamSource(QueryParamSource):
    def params(self):
        result = {
            "body": {
                "query": {
                    "match": {
                        "tag": "%s" % random.choice(self.tags)
                    }
                },
                "sort": [
                    {
                        "answers.date": {
                            "mode": "max",
                            "order": "desc",
                            "nested": {
                                "path": "answers"
                            }
                        }
                    }
                ]
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result


class TermQueryParamSource(QueryParamSource):
    def params(self):
        result = {
            "body": {
                "query": {
                    "match": {
                        "tag": "%s" % random.choice(self.tags)
                    }
                }
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result


class NestedQueryParamSource(QueryParamSource):
    def params(self):
        result = {
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "tag": "%s" % random.choice(self.tags)
                                }
                            },
                            {
                                "nested": {
                                    "path": "answers",
                                    "query": {
                                        "range": {
                                            "answers.date": {
                                                "lte": "%s" % random.choice(self.dates)
                                            }
                                        }
                                    }
                                }
                            }
                        ]
                    }
                }
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result


class NestedQueryParamSourceWithInnerHits(QueryParamSource):
    def params(self):
        result = {
            "body": {
                "query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    "tag": "%s" % random.choice(self.tags)
                                }
                            },
                            {
                                "nested": {
                                    "path": "answers",
                                    "query": {
                                        "range": {
                                            "answers.date": {
                                                "lte": "%s" % random.choice(self.dates)
                                            }
                                        }
                                    },
                                    "inner_hits": {
                                        "size": self._params["inner_hits_size"]
                                    }
                                }
                            }
                        ]
                    }
                },
                "size": self._params["size"]
            },
            "index": None
        }
        if "cache" in self._params:
            result["cache"] = self._params["cache"]

        return result


def register(registry):
    registry.register_param_source("nested-query-source", NestedQueryParamSource)
    registry.register_param_source("nested-query-source-with-inner-hits", NestedQueryParamSourceWithInnerHits)
    registry.register_param_source("term-query-source", TermQueryParamSource)
    registry.register_param_source("sorted-term-query-source", SortedTermQueryParamSource)
