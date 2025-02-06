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

from unittest import TestCase
from unittest.mock import Mock, patch

from osbenchmark.workload_generator.config import CustomWorkload
from osbenchmark.workload_generator.extractors import IndexExtractor

class TestIndexExtractor(TestCase):

    def setUp(self):
        self.mock_custom_workload = Mock(spec=CustomWorkload())
        # pylint: disable=no-value-for-parameter
        self.mock_client = self.create_mock_client()
        self.index_extractor =  IndexExtractor(self.mock_custom_workload, self.mock_client)

    @patch("opensearchpy.OpenSearch")
    def create_mock_client(self, client):
        return client

    def test_index_setting_filter(self):
        unfiltered_index_settings = {
            "number_of_shards": "5",
            "provided_name": "queries",
            "creation_date": "1579230289084",
            "requests": {
                "cache": {
                    "enable": "false"
                }
            },
            "number_of_replicas": "0",
            "queries": {
                "cache": {
                    "enabled": "false"
                }
            },
            "uuid": "jdzVt-dDS1aRlqdZWK4pdA",
            "version": {
                "created": "7050099"
            },
            "store": {
                "type": "fs"
            }
        }
        settings = self.index_extractor.filter_ephemeral_index_settings(unfiltered_index_settings)
        assert settings.keys() == {"number_of_shards", "number_of_replicas", "requests", "queries"}

    def test_index_setting_parameters(self):
        settings = {
            "number_of_shards": "5",
            "provided_name": "queries",
            "creation_date": "1579230289084",
            "requests": {
                "cache": {
                    "enable": "false"
                }
            },
            "number_of_replicas": "0",
        }
        self.index_extractor.update_index_setting_parameters(settings)
        assert settings == {
            "number_of_shards": "{{number_of_shards | default(5)}}",
            "provided_name": "queries",
            "creation_date": "1579230289084",
            "requests": {
                "cache": {
                    "enable": "false"
                }
            },
            "number_of_replicas": "{{number_of_replicas | default(0)}}",
        }
        # make sure we don't explode if the parameterized settings aren't present for some reason
        settings.pop("number_of_shards")
        settings.pop("number_of_replicas")
        self.index_extractor.update_index_setting_parameters(settings)

    def test_extract_index_create(self):
        self.mock_client.indices.get.return_value = {
            "osmgeopoints": {
                "aliases": {},
                "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                },
                "settings": {
                    "index": {
                        "number_of_shards": "3",
                        "provided_name": "osmgeopoints",
                        "creation_date": "1579210032233",
                        "requests": {
                            "cache": {
                                "enable": "false"
                            }
                        },
                        "number_of_replicas": "2",
                        "uuid": "vOOsPNfxTJyQekkIo9TjPA",
                        "version": {
                            "created": "7050099"
                        },
                        "store": {
                            "type": "fs"
                        }
                    }
                }
            },
            # should be filtered
            ".security": {
                "mappings": {},
                "settings": {
                    "index": {
                        "number_of_shards": "1"
                    }
                }
            },
            "geodata": {
                "mappings": {},
                "settings": {
                    "index": {
                        "number_of_shards": "1"
                    }
                }
            }
        }
        expected = {
            "osmgeopoints": {
                "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "location": {
                            "type": "geo_point"
                        }
                    }
                },
                "settings": {
                    "index": {
                        "number_of_replicas": "{{number_of_replicas | default(2)}}",
                        "number_of_shards": "{{number_of_shards | default(3)}}",
                        "requests": {
                            "cache": {
                                "enable": "false"
                            }
                        }
                    }
                }
            },
            "geodata": {
                "mappings": {},
                "settings": {
                    "index": {
                        "number_of_shards": "{{number_of_shards | default(1)}}"
                    }
                }
            }
        }
        res = self.index_extractor.extract_index_mapping_and_settings("_all")
        assert res == expected
