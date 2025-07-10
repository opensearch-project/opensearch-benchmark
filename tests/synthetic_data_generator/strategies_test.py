# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from unittest.mock import MagicMock, patch
import pytest

from osbenchmark.exceptions import ConfigError
from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata
from osbenchmark.synthetic_data_generator.strategies import MappingStrategy, CustomModuleStrategy
from osbenchmark.synthetic_data_generator.strategies.mapping_strategy import MappingConverter
from osbenchmark.synthetic_data_generator import helpers


class TestCustomStrategy:

    @pytest.fixture
    def setup_sdg_metadata(self):
        return SyntheticDataGeneratorMetadata(
            index_name="test-custom-module-sdg",
            index_mappings_path="path/to/mappings",
            custom_config_path="/path/to/config",
            custom_module_path="/path/to/module",
            output_path="/path/to/output",
            total_size_gb=10,
            mode=None,
            checkpoint=None,
            blueprint=None,
            generators={}
        )

    @pytest.fixture
    def mock_sdg_config(self):
        sdg_config = {
                    'settings': {'workers': 8, 'max_file_size_gb': 1, 'chunk_size': 10000},
                    'CustomGenerationValues': {
                        'custom_lists': {'dog_names': ['Hana', 'Youpie', 'Charlie', 'Lucy', 'Cooper', 'Luna', 'Rocky', 'Daisy', 'Buddy', 'Molly'],
                                        'dog_breeds': ['Jindo', 'Labrador', 'German Shepherd', 'Golden Retriever', 'Bulldog', 'Poodle', 'Beagle', 'Rottweiler', 'Boxer', 'Dachshund', 'Chihuahua'],
                                        'treats': ['cookies', 'pup_cup', 'jerky'], 'license_plates': ['WOOF101', 'BARKATAMZN'],
                                        'tips': ['biscuits', 'cash'],
                                        'skills': ['sniffing', 'squirrel_chasing', 'bite_tail', 'smile'],
                                        'vehicle_types': ['sedan', 'suv', 'truck'], 'vehicle_makes': ['toyta', 'honda', 'nissan'],
                                        'vehicle_models': ['rav4', 'accord', 'murano'], 'vehicle_years': [2012, 2015, 2019],
                                        'vehicle_colors': ['white', 'red', 'blue', 'black', 'silver'], 'account_status': ['active', 'inactive']},
                        'custom_providers': ['NumericString', 'MultipleChoices']
                    }
                }

        return sdg_config

    @pytest.fixture
    def custom_module_strategy(self, setup_sdg_metadata, mock_sdg_config):
        import os
        sample_module_path = f"{os.path.dirname(os.path.realpath(__file__))}/sample_custom_module.py"
        custom_module = helpers.load_user_module(sample_module_path)
        strategy = CustomModuleStrategy(setup_sdg_metadata, mock_sdg_config, custom_module)

        return strategy

    @pytest.fixture
    def dask_client(self, sample_docs_generated):
        dask_client = MagicMock()

        mock_futures = []
        for doc in sample_docs_generated:
            mock_future = MagicMock()
            mock_future.result.return_value = [doc] # Each worker produces a list of docs. In this test, each worker is returning a list of one doc
            mock_futures.append(mock_future)

        dask_client.submit.side_effect = mock_futures

        return dask_client

    @pytest.fixture
    def sample_docs_generated(self):
        return [
            {"dog_driver_id": "DD4444", "dog_name": "Hana", "dog_breed": "Korean Jindo",
             "license_number": "BARKATAMZN6176", "favorite_treats": "pup_cup", "preferred_tip": "cash", "vehicle_type": "truck",
             "vehicle_make": "honda", "vehicle_model": "murano", "vehicle_year": 2019, "vehicle_color": "black", "license_plate": "WOOF101",
             "current_location": {"lat": 40.73389901726337, "lon": -73.95726095278667}, "status": "available", "current_ride": "R922315",
             "account_status": "inactive", "join_date": "05/01/2017", "total_rides": 166, "rating": 1.91,
             "earnings": {"today": {"amount": 2.26, "currency": "USD"}, "this_week": {"amount": 1.44, "currency": "USD"}, "this_month": {"amount": 1.31, "currency": "USD"}},
             "last_grooming_check": "2023-12-01", "owner": {"first_name": "Elfrieda", "last_name": "Huffman", "email": "ElfriedaHuffman@gmail.com"},
             "special_skills": ["bite_tail", "bite_tail", "smile"], "bark_volume": 9.33, "tail_wag_speed": 4.5},
            {"dog_driver_id": "DD2495", "dog_name": "Luna", "dog_breed": "Chihuahua", "license_number": "WOOF1014472", "favorite_treats": "jerky",
             "preferred_tip": "cash", "vehicle_type": "sedan", "vehicle_make": "nissan", "vehicle_model": "murano", "vehicle_year": 2019, "vehicle_color": "silver",
             "license_plate": "WOOF101", "current_location": {"lat": 40.75654230013213, "lon": -73.98178219702368}, "status": "busy", "current_ride": "R690202",
             "account_status": "active", "join_date": "03/06/2018", "total_rides": 24, "rating": 2.13,
             "earnings": {"today": {"amount": 1.4, "currency": "USD"}, "this_week": {"amount": 3.89, "currency": "USD"}, "this_month": {"amount": 4.88, "currency": "USD"}},
             "last_grooming_check": "2023-12-01", "owner": {"first_name": "Avery", "last_name": "Moran", "email": "AveryMoran@gmail.com"}, "special_skills": ["sniffing", "bite_tail", "smile"],
             "bark_volume": 1.73, "tail_wag_speed": 9.1},
            {"dog_driver_id": "DD2223", "dog_name": "Youpie", "dog_breed": "Boxer", "license_number": "BARKATAMZN7147", "favorite_treats": "jerky",
             "preferred_tip": "cash", "vehicle_type": "suv", "vehicle_make": "nissan", "vehicle_model": "murano", "vehicle_year": 2015, "vehicle_color": "white",
             "license_plate": "BARKATAMZN", "current_location": {"lat": 30.212385699598567, "lon": -97.76458615057449}, "status": "available",
             "current_ride": "R303297", "account_status": "inactive", "join_date": "03/25/2025", "total_rides": 110, "rating": 1.58,
             "earnings": {"today": {"amount": 2.98, "currency": "USD"}, "this_week": {"amount": 2.71, "currency": "USD"}, "this_month": {"amount": 4.89, "currency": "USD"}},
             "last_grooming_check": "2023-12-01", "owner": {"first_name": "Otto", "last_name": "Stephens", "email": "OttoStephens@gmail.com"},
             "special_skills": ["sniffing", "squirrel_chasing", "bite_tail"], "bark_volume": 7.09, "tail_wag_speed": 7.2}
        ]

    @pytest.fixture
    def sample_field_names(self):
        return [
            "dog_driver_id",
            "dog_name",
            "dog_breed",
            "license_number",
            "favorite_treats",
            "preferred_tip",
            "vehicle_type",
            "vehicle_make",
            "vehicle_model",
            "vehicle_year",
            "vehicle_color",
            "license_plate",
            "current_location",
            "status",
            "current_ride",
            "account_status",
            "join_date",
            "total_rides",
            "rating",
            "earnings",
            "last_grooming_check",
            "owner",
            "special_skills",
            "bark_volume",
            "tail_wag_speed"
        ]

    def test_generate_test_document(self, sample_field_names, custom_module_strategy):
        document = custom_module_strategy.generate_test_document()

        for field_name in sample_field_names:
            assert field_name in document

    def test_avg_doc_size(self, custom_module_strategy):
        avg_doc_size = custom_module_strategy.calculate_avg_doc_size()
        assert isinstance(avg_doc_size, int)

    def test_generate_data_chunks_across_workers(self, dask_client, custom_module_strategy):
        futures_across_workers = custom_module_strategy.generate_data_chunks_across_workers(dask_client, 3, [1,2,3])
        docs = [future.result() for future in futures_across_workers]

        assert len(docs) == 3
        assert "Hana" == docs[0][0]["dog_name"]
        assert "Luna" == docs[1][0]["dog_name"]
        assert "Youpie" == docs[2][0]["dog_name"]

    def test_generate_data_chunk_from_worker(self, sample_field_names, custom_module_strategy):
        user_defined_function = custom_module_strategy.custom_module.generate_fake_document
        data_chunk = custom_module_strategy.generate_data_chunk_from_worker(user_defined_function, 3, 12345)

        assert len(data_chunk) == 3
        assert isinstance(data_chunk, list)
        for document in data_chunk:
            for field in sample_field_names:
                assert field in document

    def test_custom_module_missing_generate_fake_document_function(self, setup_sdg_metadata, mock_sdg_config):
        import os
        import re
        sample_module_path = f"{os.path.dirname(os.path.realpath(__file__))}/incorrect_sample_custom_module.py"
        custom_module = helpers.load_user_module(sample_module_path)

        error_msg_expected = re.escape("Custom module at [/path/to/module] does not define a function called generate_fake_document(). Ensure that this method is defined.")
        with pytest.raises(ConfigError, match=error_msg_expected):
            strategy = CustomModuleStrategy(setup_sdg_metadata, mock_sdg_config, custom_module)


class TestMappingStrategy:

    @pytest.fixture
    def setup_sdg_metadata(self):
        return SyntheticDataGeneratorMetadata(
            index_name="test-custom-module-sdg",
            index_mappings_path="path/to/mappings",
            custom_config_path="/path/to/config",
            custom_module_path="/path/to/module",
            output_path="/path/to/output",
            total_size_gb=10,
            mode=None,
            checkpoint=None,
            blueprint=None,
            generators={}
        )

    @pytest.fixture
    def mock_sdg_config(self):
        sdg_config = {
                    'settings': {'workers': 8, 'max_file_size_gb': 1, 'chunk_size': 10000},
                    'MappingGenerationValues': {
                        'generator_overrides': {
                            'integer': {'min': 0, 'max': 20},
                            'long': {'min': 0, 'max': 1000},
                            'float': {'min': 0.0, 'max': 1.0},
                            'double': {'min': 0.0, 'max': 2000.0},
                            'date': {'start_date': '2020-01-01', 'end_date': '2023-01-01', 'format': 'yyyy-mm-dd'},
                            'text': {'must_include': ['lorem', 'ipsum']},
                            'keyword': {'choices': ['naruto', 'sakura', 'sasuke']}
                        },
                        'field_overrides': {
                            'id': {'generator': 'generate_keyword',
                            'params': {'choices': ['Helly R', 'Mark S', 'Irving B']}},
                            'promo_codes': {'generator': 'generate_keyword', 'params': {'choices': ['HOT_SUMMER', 'TREATSYUM!']}},
                            'preferences.language': {'generator': 'generate_keyword', 'params': {'choices': ['Python', 'English']}},
                            'payment_methods.type': {'generator': 'generate_keyword', 'params': {'choices': ['Visa', 'Mastercard', 'Cash', 'Venmo']}},
                            'preferences.allergies': {'generator': 'generate_keyword', 'params': {'choices': ['Squirrels', 'Cats']}},
                            'favorite_locations.name': {'generator': 'generate_keyword', 'params': {'choices': ['Austin', 'NYC', 'Miami']}}
                        }
                    }
                }

        return sdg_config

    @pytest.fixture
    def basic_opensearch_index_mappings(self):
        return {
            "mappings": {
                "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "standard",
                    "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                    }
                },
                "description": {
                    "type": "text"
                },
                "price": {
                    "type": "float"
                },
                "created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                "is_available": {
                    "type": "boolean"
                },
                "category_id": {
                    "type": "integer"
                },
                "tags": {
                    "type": "keyword"
                }
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            }
        }

    @pytest.fixture
    def complex_opensearch_index_mapping(self):
        return {
                    "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "user": {
                        "type": "object",
                        "properties": {
                            "id": {
                            "type": "keyword"
                            },
                            "email": {
                            "type": "keyword"
                            },
                            "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                                },
                                "completion": {
                                "type": "completion"
                                }
                            },
                            "analyzer": "standard"
                            },
                            "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                "type": "text"
                                },
                                "city": {
                                "type": "keyword"
                                },
                                "state": {
                                "type": "keyword"
                                },
                                "zip": {
                                "type": "keyword"
                                },
                                "location": {
                                "type": "geo_point"
                                }
                            }
                            },
                            "preferences": {
                            "type": "object",
                            "dynamic": True
                            }
                        }
                        },
                        "orders": {
                        "type": "nested",
                        "properties": {
                            "id": {
                            "type": "keyword"
                            },
                            "date": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis"
                            },
                            "amount": {
                            "type": "scaled_float",
                            "scaling_factor": 100
                            },
                            "status": {
                            "type": "keyword"
                            },
                            "items": {
                            "type": "nested",
                            "properties": {
                                "product_id": {
                                "type": "keyword"
                                },
                                "name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                    "type": "keyword"
                                    }
                                }
                                },
                                "quantity": {
                                "type": "short"
                                },
                                "price": {
                                "type": "float"
                                },
                                "categories": {
                                "type": "keyword"
                                }
                            }
                            },
                            "shipping_address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                "type": "text"
                                },
                                "city": {
                                "type": "keyword"
                                },
                                "state": {
                                "type": "keyword"
                                },
                                "zip": {
                                "type": "keyword"
                                },
                                "location": {
                                "type": "geo_point"
                                }
                            }
                            }
                        }
                        },
                        "activity_log": {
                        "type": "nested",
                        "properties": {
                            "timestamp": {
                            "type": "date"
                            },
                            "action": {
                            "type": "keyword"
                            },
                            "ip_address": {
                            "type": "ip"
                            },
                            "details": {
                            "type": "object",
                            "enabled": False
                            }
                        }
                        },
                        "metadata": {
                        "type": "object",
                        "properties": {
                            "created_at": {
                            "type": "date"
                            },
                            "updated_at": {
                            "type": "date"
                            },
                            "tags": {
                            "type": "keyword"
                            },
                            "source": {
                            "type": "keyword"
                            },
                            "version": {
                            "type": "integer"
                            }
                        }
                        },
                        "description": {
                        "type": "text",
                        "analyzer": "english",
                        "fields": {
                            "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                            },
                            "standard": {
                            "type": "text",
                            "analyzer": "standard"
                            }
                        }
                        },
                        "ranking_scores": {
                        "type": "object",
                        "properties": {
                            "popularity": {
                            "type": "float"
                            },
                            "relevance": {
                            "type": "float"
                            },
                            "quality": {
                            "type": "float"
                            }
                        }
                        },
                        "permissions": {
                        "type": "nested",
                        "properties": {
                            "user_id": {
                            "type": "keyword"
                            },
                            "role": {
                            "type": "keyword"
                            },
                            "granted_at": {
                            "type": "date"
                            }
                        }
                        }
                    }
                    },
                    "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 2,
                    "analysis": {
                        "analyzer": {
                        "email_analyzer": {
                            "type": "custom",
                            "tokenizer": "uax_url_email",
                            "filter": ["lowercase", "stop"]
                        }
                        }
                    }
                    }
                }

    @pytest.fixture
    def sample_docs_for_basic_mappings(self):
        return [
            {"title": "ipsum Sample text for 70", "description": "lorem Sample text for 94", "price": 0.0, "created_at": "2020-04-22", "is_available": False, "category_id": 13, "tags": "Mark S"},
            {"title": "lorem Sample text for 7", "description": "lorem Sample text for 68", "price": 0.78, "created_at": "2022-07-04", "is_available": True, "category_id": 10, "tags": "Helly R"},
            {"title": "lorem Sample text for 87", "description": "ipsum Sample text for 99", "price": 0.91, "created_at": "2020-02-21", "is_available": True, "category_id": 16, "tags": "Irving B"}
        ]

    @pytest.fixture
    def sample_docs_for_complex_mappings(self):
        return [
            {"user": {"id": "sakura", "email": "naruto", "name": "lorem Sample text for 87", "address": {"street": "lorem Sample text for 34", "city": "naruto", "state": "sasuke", "zip": "naruto", "location": {"lat": 26.99576279125438, "lon": -106.55835561335948}}, "preferences": {}}, "orders": [{"id": "sakura", "date": "2021-11-17", "amount": "unknown_type", "status": "naruto", "items": [{"product_id": "sasuke", "name": "ipsum Sample text for 5", "quantity": 19395, "price": 0.31, "categories": "sakura"}, {"product_id": "sakura", "name": "lorem Sample text for 94", "quantity": -5488, "price": 0.76, "categories": "sasuke"}], "shipping_address": {"street": "lorem Sample text for 63", "city": "naruto", "state": "sasuke", "zip": "sakura", "location": {"lat": 40.62216376082151, "lon": -65.29355206583621}}}, {"id": "sakura", "date": "2021-03-05", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "sakura", "name": "lorem Sample text for 100", "quantity": -1063, "price": 0.11, "categories": "sakura"}], "shipping_address": {"street": "lorem Sample text for 72", "city": "sasuke", "state": "naruto", "zip": "sasuke", "location": {"lat": 64.92051504356562, "lon": -64.90676398234942}}}, {"id": "naruto", "date": "2020-04-23", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "sasuke", "name": "lorem Sample text for 5", "quantity": -27595, "price": 0.73, "categories": "sasuke"}, {"product_id": "sasuke", "name": "ipsum Sample text for 8", "quantity": 3581, "price": 0.65, "categories": "naruto"}], "shipping_address": {"street": "lorem Sample text for 30", "city": "sasuke", "state": "sakura", "zip": "naruto", "location": {"lat": -48.34559264417752, "lon": -178.36558923535966}}}], "activity_log": [{"timestamp": "2021-09-22", "action": "sakura", "ip_address": "101.52.247.55", "details": {}}, {"timestamp": "2022-01-22", "action": "sasuke", "ip_address": "44.189.12.245", "details": {}}, {"timestamp": "2022-07-03", "action": "naruto", "ip_address": "131.232.186.58", "details": {}}], "metadata": {"created_at": "2022-02-09", "updated_at": "2022-04-18", "tags": "sasuke", "source": "sasuke", "version": 6}, "description": "lorem Sample text for 46", "ranking_scores": {"popularity": 0.89, "relevance": 0.79, "quality": 0.95}, "permissions": [{"user_id": "sasuke", "role": "naruto", "granted_at": "2022-09-18"}]},
            {"user": {"id": "naruto", "email": "sakura", "name": "ipsum Sample text for 62", "address": {"street": "ipsum Sample text for 69", "city": "sasuke", "state": "naruto", "zip": "naruto", "location": {"lat": 18.716170289552792, "lon": 26.676759590182087}}, "preferences": {}}, "orders": [{"id": "sakura", "date": "2020-03-14", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "naruto", "name": "ipsum Sample text for 72", "quantity": 8096, "price": 0.98, "categories": "sasuke"}], "shipping_address": {"street": "ipsum Sample text for 70", "city": "sasuke", "state": "naruto", "zip": "sasuke", "location": {"lat": 35.160379130894796, "lon": 142.70658997708557}}}, {"id": "sasuke", "date": "2021-04-10", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "sakura", "name": "ipsum Sample text for 95", "quantity": -26888, "price": 0.9, "categories": "sasuke"}, {"product_id": "sakura", "name": "lorem Sample text for 77", "quantity": -4878, "price": 0.68, "categories": "sakura"}, {"product_id": "sasuke", "name": "lorem Sample text for 60", "quantity": 3465, "price": 0.92, "categories": "sakura"}], "shipping_address": {"street": "ipsum Sample text for 69", "city": "sakura", "state": "sasuke", "zip": "sakura", "location": {"lat": -10.880093403565638, "lon": -167.79823045612983}}}, {"id": "sasuke", "date": "2021-11-06", "amount": "unknown_type", "status": "sakura", "items": [{"product_id": "sakura", "name": "lorem Sample text for 55", "quantity": -22916, "price": 0.29, "categories": "naruto"}, {"product_id": "sasuke", "name": "lorem Sample text for 72", "quantity": -5256, "price": 0.45, "categories": "naruto"}], "shipping_address": {"street": "ipsum Sample text for 76", "city": "naruto", "state": "naruto", "zip": "sasuke", "location": {"lat": 89.19863809212737, "lon": 114.09913348615743}}}], "activity_log": [{"timestamp": "2020-06-26", "action": "naruto", "ip_address": "139.179.72.223", "details": {}}], "metadata": {"created_at": "2020-02-11", "updated_at": "2021-05-25", "tags": "sasuke", "source": "sakura", "version": 16}, "description": "ipsum Sample text for 8", "ranking_scores": {"popularity": 0.16, "relevance": 0.06, "quality": 0.72}, "permissions": [{"user_id": "sakura", "role": "sasuke", "granted_at": "2020-01-23"}, {"user_id": "sasuke", "role": "naruto", "granted_at": "2022-11-19"}, {"user_id": "sakura", "role": "naruto", "granted_at": "2021-07-06"}]},
            {"user": {"id": "sakura", "email": "naruto", "name": "lorem Sample text for 57", "address": {"street": "ipsum Sample text for 83", "city": "sasuke", "state": "sasuke", "zip": "sakura", "location": {"lat": 32.588564110838846, "lon": -23.052179676898845}}, "preferences": {}}, "orders": [{"id": "sakura", "date": "2021-03-21", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "sasuke", "name": "ipsum Sample text for 63", "quantity": 7348, "price": 0.37, "categories": "sasuke"}], "shipping_address": {"street": "ipsum Sample text for 34", "city": "naruto", "state": "sakura", "zip": "sasuke", "location": {"lat": -51.05656520056002, "lon": -17.084501461214813}}}, {"id": "sakura", "date": "2021-01-21", "amount": "unknown_type", "status": "sasuke", "items": [{"product_id": "naruto", "name": "ipsum Sample text for 43", "quantity": 28814, "price": 0.91, "categories": "sakura"}, {"product_id": "sasuke", "name": "ipsum Sample text for 80", "quantity": -293, "price": 0.52, "categories": "naruto"}, {"product_id": "sakura", "name": "ipsum Sample text for 72", "quantity": -31854, "price": 0.54, "categories": "sakura"}], "shipping_address": {"street": "lorem Sample text for 19", "city": "sakura", "state": "sasuke", "zip": "sasuke", "location": {"lat": 40.539565425667945, "lon": 59.77455111107835}}}], "activity_log": [{"timestamp": "2021-04-14", "action": "sakura", "ip_address": "155.140.52.23", "details": {}}], "metadata": {"created_at": "2021-06-22", "updated_at": "2020-09-06", "tags": "naruto", "source": "naruto", "version": 9}, "description": "ipsum Sample text for 55", "ranking_scores": {"popularity": 0.49, "relevance": 0.23, "quality": 0.32}, "permissions": [{"user_id": "sakura", "role": "sasuke", "granted_at": "2020-07-29"}, {"user_id": "naruto", "role": "sasuke", "granted_at": "2021-01-19"}]}
        ]


    @pytest.fixture
    def mapping_strategy_with_basic_mappings(self, setup_sdg_metadata, mock_sdg_config, basic_opensearch_index_mappings):
        strategy = MappingStrategy(setup_sdg_metadata, mock_sdg_config, basic_opensearch_index_mappings)

        return strategy


    @pytest.fixture
    def mapping_strategy_with_complex_mappings(self, setup_sdg_metadata, mock_sdg_config, complex_opensearch_index_mappings):
        strategy = MappingStrategy(setup_sdg_metadata, mock_sdg_config, complex_opensearch_index_mappings)

        return strategy

    @pytest.fixture
    def dask_client(self, sample_docs_for_basic_mappings):
        dask_client = MagicMock()

        mock_futures = []
        for doc in sample_docs_for_basic_mappings:
            mock_future = MagicMock()
            mock_future.result.return_value = [doc] # Each worker produces a list of docs. In this test, each worker is returning a list of one doc
            mock_futures.append(mock_future)

        dask_client.submit.side_effect = mock_futures

        return dask_client


    def test_generate_test_document(self, mapping_strategy_with_basic_mappings):
        field_names = ["title", "description", "price", "created_at", "is_available", "category_id", "tags"]
        document = mapping_strategy_with_basic_mappings.generate_test_document()

        for field_name in field_names:
            assert field_name in document

    def test_avg_doc_size(self, mapping_strategy_with_basic_mappings):
        avg_doc_size = mapping_strategy_with_basic_mappings.calculate_avg_doc_size()
        assert isinstance(avg_doc_size, int)

    def test_generate_data_chunks_across_workers(self, dask_client, mapping_strategy_with_basic_mappings):
        fields = ["title", "description", "price", "created_at", "is_available", "category_id", "tags"]
        futures_across_workers = mapping_strategy_with_basic_mappings.generate_data_chunks_across_workers(dask_client, 3, [1,2,3])
        docs = [future.result() for future in futures_across_workers]

        assert len(docs) == 3
        assert "Mark S" == docs[0][0]["tags"]
        assert "Helly R" == docs[1][0]["tags"]
        assert "Irving B" == docs[2][0]["tags"]

        for doc in docs:
            for field in fields:
                assert field in doc[0]

    def test_generate_data_chunk_from_worker(self, mapping_strategy_with_basic_mappings):
        fields = ["title", "description", "price", "created_at", "is_available", "category_id", "tags"]

        data_chunk = mapping_strategy_with_basic_mappings.generate_data_chunk_from_worker(3, 12345)

        assert len(data_chunk) == 3
        assert isinstance(data_chunk, list)

        for doc in data_chunk:
            for field in fields:
                assert field in doc

class TestMappingConverter:

    @pytest.fixture
    def mock_sdg_config(self):
        sdg_config = {
                    'settings': {'workers': 8, 'max_file_size_gb': 1, 'chunk_size': 10000},
                    'MappingGenerationValues': {
                        'generator_overrides': {
                            'integer': {'min': 0, 'max': 20},
                            'long': {'min': 0, 'max': 1000},
                            'float': {'min': 0.0, 'max': 1.0},
                            'double': {'min': 0.0, 'max': 2000.0},
                            'date': {'start_date': '2020-01-01', 'end_date': '2023-01-01', 'format': 'yyyy-mm-dd'},
                            'text': {'must_include': ['lorem', 'ipsum']},
                            'keyword': {'choices': ['naruto', 'sakura', 'sasuke']}
                        },
                        'field_overrides': {
                            'id': {'generator': 'generate_keyword',
                                'params': {'choices': ['Helly R', 'Mark S', 'Irving B']}},
                            'promo_codes': {'generator': 'generate_keyword', 'params': {'choices': ['HOT_SUMMER', 'TREATSYUM!']}},
                            'preferences.language': {'generator': 'generate_keyword', 'params': {'choices': ['Python', 'English']}},
                            'payment_methods.type': {'generator': 'generate_keyword', 'params': {'choices': ['Visa', 'Mastercard', 'Cash', 'Venmo']}},
                            'preferences.allergies': {'generator': 'generate_keyword', 'params': {'choices': ['Squirrels', 'Cats']}},
                            'favorite_locations.name': {'generator': 'generate_keyword', 'params': {'choices': ['Austin', 'NYC', 'Miami']}}
                        }
                    }
                }

        return sdg_config

    @pytest.fixture
    def basic_opensearch_index_mappings(self):
        return {
            "mappings": {
                "properties": {
                "title": {
                    "type": "text",
                    "analyzer": "standard",
                    "fields": {
                    "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                    }
                    }
                },
                "description": {
                    "type": "text"
                },
                "price": {
                    "type": "float"
                },
                "created_at": {
                    "type": "date",
                    "format": "strict_date_optional_time||epoch_millis"
                },
                "is_available": {
                    "type": "boolean"
                },
                "category_id": {
                    "type": "integer"
                },
                "tags": {
                    "type": "keyword"
                }
                }
            },
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            }
        }

    @pytest.fixture
    def complex_opensearch_index_mappings(self):
        return {
                    "mappings": {
                    "dynamic": "strict",
                    "properties": {
                        "user": {
                        "type": "object",
                        "properties": {
                            "id": {
                            "type": "keyword"
                            },
                            "email": {
                            "type": "keyword"
                            },
                            "name": {
                            "type": "text",
                            "fields": {
                                "keyword": {
                                "type": "keyword",
                                "ignore_above": 256
                                },
                                "completion": {
                                "type": "completion"
                                }
                            },
                            "analyzer": "standard"
                            },
                            "address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                "type": "text"
                                },
                                "city": {
                                "type": "keyword"
                                },
                                "state": {
                                "type": "keyword"
                                },
                                "zip": {
                                "type": "keyword"
                                },
                                "location": {
                                "type": "geo_point"
                                }
                            }
                            },
                            "preferences": {
                            "type": "object",
                            "dynamic": True
                            }
                        }
                        },
                        "orders": {
                        "type": "nested",
                        "properties": {
                            "id": {
                            "type": "keyword"
                            },
                            "date": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis"
                            },
                            "amount": {
                            "type": "scaled_float",
                            "scaling_factor": 100
                            },
                            "status": {
                            "type": "keyword"
                            },
                            "items": {
                            "type": "nested",
                            "properties": {
                                "product_id": {
                                "type": "keyword"
                                },
                                "name": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                    "type": "keyword"
                                    }
                                }
                                },
                                "quantity": {
                                "type": "short"
                                },
                                "price": {
                                "type": "float"
                                },
                                "categories": {
                                "type": "keyword"
                                }
                            }
                            },
                            "shipping_address": {
                            "type": "object",
                            "properties": {
                                "street": {
                                "type": "text"
                                },
                                "city": {
                                "type": "keyword"
                                },
                                "state": {
                                "type": "keyword"
                                },
                                "zip": {
                                "type": "keyword"
                                },
                                "location": {
                                "type": "geo_point"
                                }
                            }
                            }
                        }
                        },
                        "activity_log": {
                        "type": "nested",
                        "properties": {
                            "timestamp": {
                            "type": "date"
                            },
                            "action": {
                            "type": "keyword"
                            },
                            "ip_address": {
                            "type": "ip"
                            },
                            "details": {
                            "type": "object",
                            "enabled": False
                            }
                        }
                        },
                        "metadata": {
                        "type": "object",
                        "properties": {
                            "created_at": {
                            "type": "date"
                            },
                            "updated_at": {
                            "type": "date"
                            },
                            "tags": {
                            "type": "keyword"
                            },
                            "source": {
                            "type": "keyword"
                            },
                            "version": {
                            "type": "integer"
                            }
                        }
                        },
                        "description": {
                        "type": "text",
                        "analyzer": "english",
                        "fields": {
                            "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                            },
                            "standard": {
                            "type": "text",
                            "analyzer": "standard"
                            }
                        }
                        },
                        "ranking_scores": {
                        "type": "object",
                        "properties": {
                            "popularity": {
                            "type": "float"
                            },
                            "relevance": {
                            "type": "float"
                            },
                            "quality": {
                            "type": "float"
                            }
                        }
                        },
                        "permissions": {
                        "type": "nested",
                        "properties": {
                            "user_id": {
                            "type": "keyword"
                            },
                            "role": {
                            "type": "keyword"
                            },
                            "granted_at": {
                            "type": "date"
                            }
                        }
                        }
                    }
                    },
                    "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 2,
                    "analysis": {
                        "analyzer": {
                        "email_analyzer": {
                            "type": "custom",
                            "tokenizer": "uax_url_email",
                            "filter": ["lowercase", "stop"]
                        }
                        }
                    }
                    }
                }

    @pytest.fixture
    def mapping_converter(self, mock_sdg_config):
        mapping_generation_values = mock_sdg_config.get("MappingGenerationValues", {})
        mapping_converter_logic = MappingConverter(mapping_generation_values, 12345)

        return mapping_converter_logic

    def test_generating_documents_from_basic_mappings(self, mapping_converter, basic_opensearch_index_mappings):
        mappings_with_generators = mapping_converter.transform_mapping_to_generators(basic_opensearch_index_mappings)

        document = MappingConverter.generate_fake_document(transformed_mapping=mappings_with_generators)

        fields = ["title", "description", "price", "created_at", "is_available", "category_id", "tags"]
        for field in fields:
            assert field in document


    def test_generating_documents_for_complex_mappings(self, mapping_converter, complex_opensearch_index_mappings):
        mappings_with_generators = mapping_converter.transform_mapping_to_generators(complex_opensearch_index_mappings)

        document = MappingConverter.generate_fake_document(transformed_mapping=mappings_with_generators)

        fields = ["user", "orders", "activity_log", "metadata", "description", "ranking_scores", "permissions"]
        for field in fields:
            assert field in document

    def test_generating_documents_for_with_overrides(self, mapping_converter):
        basic_mappings = {
            "properties": {
                "id": {
                    "type": "keyword"
                },
                "amount": {
                    "type": "float"
                },
                "created_at": {
                    "type": "date"
                },
                "status": {
                    "type": "keyword"
                }
            }
        }

        mappings_with_generators_and_overrides = mapping_converter.transform_mapping_to_generators(basic_mappings)
        document = MappingConverter.generate_fake_document(transformed_mapping=mappings_with_generators_and_overrides)

        fields = ["id", "amount", "created_at", "status"]

        for field in fields:
            assert field in document

        assert document["id"] in ["Helly R", "Mark S", "Irving B"]
