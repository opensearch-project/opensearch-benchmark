from unittest.mock import patch, MagicMock, mock_open
import pytest
from datetime import datetime

from osbenchmark.synthetic_data_generator.mapping_synthetic_data_generator import MappingSyntheticDataGenerator, MappingSyntheticDataGeneratorWorker
from osbenchmark.exceptions import ConfigError

class TestMappingSyntheticDataGenerator:

    @pytest.fixture
    def sample_mapping(self):
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
                    },
                    "author_profile": {
                        "type": "object",
                        "properties": {
                            "bio": {"type": "text"},
                            "email": {"type": "keyword"}
                        }
                    },
                    "other_works": {
                        "type": "nested",
                        "properties": {
                            "title": {"type": "text"}
                        }
                    }
                }
            }
        }

    @pytest.fixture
    def sample_config(self):
        return {
            "MappingSyntheticDataGenerator": {
                "generator_overrides": {
                    "integer": {"min": 1, "max": 100},
                    "float": {"min": 0, "max": 10, "round": 1}
                },
                "field_overrides": {
                    "title": {
                        "generator": "generate_text",
                        "params": {
                            "must_include": ["Mark S"]
                        }
                    },
                    "tags": {
                        "generator": "generate_keyword",
                        "params": {
                            "choices": ["monet"]
                        }
                    },
                    "other_works.title": {
                        "generator": "generate_text",
                        "params": {
                            "must_include": ["Mark S"]
                        }
                    },
                    "author_profile.email": {
                        "generator": "generate_keyword",
                        "params": {
                            "choices": ["marktwain@gmail.com"]
                        }
                    }
                }
            }
        }

    @pytest.fixture
    def mapping_synthetic_data_generator(self):
        return MappingSyntheticDataGenerator()

    def test_that_mapping_synthetic_data_generator_intializes_correctly(self, mapping_synthetic_data_generator):
        assert mapping_synthetic_data_generator is not None
        assert hasattr(mapping_synthetic_data_generator, 'type_generators')
        assert isinstance(mapping_synthetic_data_generator.type_generators, dict)

    def test_generate_text(self, mapping_synthetic_data_generator):
        field_definition = {"type": "text"}

        text = mapping_synthetic_data_generator.generate_text(field_definition)

        assert isinstance(text, str)
        assert "Sample text for" in text

        # Test other params like must_include
        text_with_must_include = mapping_synthetic_data_generator.generate_text(field_definition, must_include=["vincent_van_gogh"])
        assert isinstance(text_with_must_include, str)
        assert "vincent_van_gogh" in text_with_must_include

        # Test analyzer keyword
        field_definition_with_keyword_analyzer = {"type": "text", "analyzer": "keyword"}
        text_with_keyword_analyzer = mapping_synthetic_data_generator.generate_text(field_definition_with_keyword_analyzer)
        assert isinstance(text_with_keyword_analyzer, str)
        assert "keyword_" in text_with_keyword_analyzer

    def test_generate_keyword(self, mapping_synthetic_data_generator):
        field_definition = {"type": "keyword"}

        # Test basic
        keyword = mapping_synthetic_data_generator.generate_keyword(field_definition)
        assert isinstance(keyword, str)
        assert "key_" in keyword

        # Test with choices
        artist_choices = ["vincent_van_gogh", "rembrandt", "monet"]
        keyword = mapping_synthetic_data_generator.generate_keyword(field_definition, choices=artist_choices)
        assert isinstance(keyword, str)
        assert keyword in artist_choices

    def test_generate_long(self, mapping_synthetic_data_generator):
        field_definition = {"type": "long"}

        long = mapping_synthetic_data_generator.generate_long(field_definition)
        assert isinstance(long, int)

    def test_generate_integer(self, mapping_synthetic_data_generator):
        field_definition = {"type": "integer"}

        # Test basic
        integer = mapping_synthetic_data_generator.generate_integer(field_definition)
        assert isinstance(integer, int)

        # Test with min and max
        integer_with_min_and_max = mapping_synthetic_data_generator.generate_integer(field_definition, min=1, max=10)
        assert isinstance(integer_with_min_and_max, int)
        assert integer_with_min_and_max <= 10 and integer_with_min_and_max >= 1

    def test_generate_short(self, mapping_synthetic_data_generator):
        field_definition = {"type": "short"}

        short = mapping_synthetic_data_generator.generate_short(field_definition)
        assert isinstance(short, int)

    def test_generate_byte(self, mapping_synthetic_data_generator):
        field_definition = {"type": "byte"}

        byte = mapping_synthetic_data_generator.generate_byte(field_definition)
        assert isinstance(byte, int)

    def test_generate_double(self, mapping_synthetic_data_generator):
        field_definition = {"type": "double"}

        double = mapping_synthetic_data_generator.generate_double(field_definition)
        assert isinstance(double, float)

    def test_generate_float(self, mapping_synthetic_data_generator):
        field_definition = {"type": "float"}

        # Test basic
        float_result = mapping_synthetic_data_generator.generate_float(field_definition)
        assert isinstance(float_result, float)

        # Test with choices
        float_result = mapping_synthetic_data_generator.generate_float(field_definition, min=1, max=10)
        assert isinstance(float_result, float)
        assert float_result <= 10 and float_result >= 1

    def test_generate_boolean(self, mapping_synthetic_data_generator):
        field_definition = {"type": "boolean"}

        boolean = mapping_synthetic_data_generator.generate_boolean(field_definition)
        assert isinstance(boolean, bool)

    def test_generate_date(self, mapping_synthetic_data_generator):
        field_definition = {"type": "date"}

        # Test basic
        date = mapping_synthetic_data_generator.generate_date(field_definition)
        assert isinstance(date, str)

        # Test with start date and end date
        date_format = "yyyy-mm-dd"
        start_date = "2010-01-01"
        end_date = "2020-01-01"

        date = mapping_synthetic_data_generator.generate_date(field_definition, date_format=date_format, start_date=start_date, end_date=end_date)

        start_date_formatted = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_formatted = datetime.strptime(end_date, "%Y-%m-%d").date()
        generated_date_formatted = datetime.strptime(date, "%Y-%m-%d").date()

        assert isinstance(date, str)
        assert start_date_formatted <= generated_date_formatted <= end_date_formatted

    def test_generate_ip(self, mapping_synthetic_data_generator):
        field_definition = {"type": "ip"}

        ip = mapping_synthetic_data_generator.generate_ip(field_definition)
        assert isinstance(ip, str)
        assert len(ip.split(".")) == 4

    def test_generate_object(self, mapping_synthetic_data_generator):
        nested_generators = {
            "author": lambda: "Mark Twain",
            "text": lambda: "What's for dinner kids?"
        }
        generated_object = mapping_synthetic_data_generator._generate_obj({}, nested_generators)

        assert isinstance(generated_object, dict)
        assert generated_object["author"] == "Mark Twain"
        assert generated_object["text"] == "What's for dinner kids?"

    def test_generate_nested_array(self, mapping_synthetic_data_generator):
        nested_generators = {
            "author": lambda: "Mark Twain",
            "text": lambda: "What's for dinner kids?"
        }
        nested_array = mapping_synthetic_data_generator._generate_nested_array({}, nested_generators, min_items=5, max_items=5)

        assert isinstance(nested_array, list)
        assert len(nested_array) == 5
        assert nested_array[0]["author"] == "Mark Twain"
        assert nested_array[0]["text"] == "What's for dinner kids?"

    def test_generate_geo_point(self, mapping_synthetic_data_generator):
        field_definition = {"type": "geo_point"}

        geo_point = mapping_synthetic_data_generator.generate_geo_point(field_definition)
        assert isinstance(geo_point, dict)
        assert -90 <= geo_point["lat"] <= 90
        assert -180 <= geo_point["lon"] <= 180

    def test_transform_mapping_to_generators(self, mapping_synthetic_data_generator, sample_mapping):
        transformed_mapping = mapping_synthetic_data_generator.transform_mapping_to_generators(sample_mapping)

        assert "title" in transformed_mapping
        assert "description" in transformed_mapping
        assert "price" in transformed_mapping
        assert "created_at" in transformed_mapping
        assert "is_available" in transformed_mapping
        assert "category_id" in transformed_mapping
        assert "tags" in transformed_mapping
        assert "author_profile" in transformed_mapping
        assert "other_works" in transformed_mapping

        assert callable(transformed_mapping["title"])
        assert callable(transformed_mapping["description"])
        assert callable(transformed_mapping["price"])
        assert callable(transformed_mapping["created_at"])
        assert callable(transformed_mapping["is_available"])
        assert callable(transformed_mapping["category_id"])
        assert callable(transformed_mapping["tags"])
        assert callable(transformed_mapping["author_profile"])
        assert callable(transformed_mapping["other_works"])

    def test_transform_mapping_to_generators_with_config(self, sample_mapping, sample_config):
        mapping_synthetic_data_generator = MappingSyntheticDataGenerator(sample_config)
        transformed_mapping = mapping_synthetic_data_generator.transform_mapping_to_generators(sample_mapping)

        # Type overrides
        price_value = transformed_mapping["price"]()
        assert 1 <= price_value <= 10

        # Field overrides
        tags_value = transformed_mapping["tags"]()
        title_value = transformed_mapping["title"]()

        assert "monet" in tags_value
        assert "Mark S" in title_value

        # Nested field overrides
        other_works = transformed_mapping["other_works"]()
        for work in other_works:
            assert "Mark S" in work["title"]

        author_profile = transformed_mapping["author_profile"]()
        assert "marktwain@gmail.com" == author_profile["email"]

    def test_generate_fake_document(self, mapping_synthetic_data_generator, sample_mapping):
        transformed_mapping = mapping_synthetic_data_generator.transform_mapping_to_generators(sample_mapping)
        fake_document = mapping_synthetic_data_generator.generate_fake_document(transformed_mapping)

        assert isinstance(fake_document, dict)
        assert "title" in fake_document
        assert "description" in fake_document
        assert "price" in fake_document
        assert "created_at" in fake_document
        assert "is_available" in fake_document
        assert "category_id" in fake_document
        assert "tags" in fake_document
        assert "author_profile" in fake_document
        assert "other_works" in fake_document

        if fake_document["author_profile"]:
            assert "bio" in fake_document["author_profile"]
            assert "email" in fake_document["author_profile"]

        if fake_document["other_works"]:
            first_work = fake_document["other_works"][0]
            assert "title" in first_work

    def test_invalid_generator_type(self, sample_mapping):
        # If a user overrides a field with an invalid field override, mapping SDG should throw an error
        invalid_config = {
            "MappingSyntheticDataGenerator": {
                "generator_overrides": {
                    "integer": {"min": 1, "max": 100},
                    "float": {"min": 0, "max": 10, "round": 1}
                },
                "field_overrides": {
                    "title": {
                        "generator": "generate_invalid_type",
                        "params": {
                            "must_include": ["Mark S"]
                        }
                    }
                }
            }
        }

        mapping_synthetic_data_generator = MappingSyntheticDataGenerator(invalid_config)

        with pytest.raises(ConfigError):
            mapping_synthetic_data_generator.transform_mapping_to_generators(sample_mapping)

class TestMappingSyntheticDataGeneratorWorker:

    @pytest.fixture
    def sample_mapping(self):
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
                    "category_id": {
                        "type": "integer"
                    }
                }
            }
        }

    @pytest.fixture
    def sample_config(self):
        return {
            "MappingSyntheticDataGenerator": {
                "generator_overrides": {
                    "integer": {"min": 1, "max": 10}
                }
            }
        }

    def test_generate_documents_from_workers(self, sample_mapping, sample_config):
        chunk_size = 5
        documents = MappingSyntheticDataGeneratorWorker.generate_documents_from_worker(
            index_mappings=sample_mapping,
            mapping_config=sample_config,
            chunk_size=chunk_size
        )

        assert isinstance(documents, list)
        assert len(documents) == 5

        for document in documents:
            assert isinstance(document, dict)
            assert "title" in document
            assert isinstance(document["title"], str)
            assert "category_id" in document
            assert isinstance(document["category_id"], int)

            assert 1 <= document["category_id"] <= 10
