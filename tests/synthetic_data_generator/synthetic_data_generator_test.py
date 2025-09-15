# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from unittest.mock import MagicMock, patch
import pytest

from osbenchmark.synthetic_data_generator.synthetic_data_generator import SyntheticDataGenerator
from osbenchmark.synthetic_data_generator.models import SyntheticDataGeneratorMetadata, SDGConfig

class TestSyntheticDataGeneratorWithCustomStrategy:

    @pytest.fixture
    def setup_sdg_metadata(self):
        return SyntheticDataGeneratorMetadata(
            index_name="test-custom-module-sdg",
            index_mappings_path="path/to/mappings",
            custom_config_path="/path/to/config",
            custom_module_path="/path/to/module",
            output_path="/path/to/output",
            total_size_gb=10
        )

    @pytest.fixture
    def mock_sdg_config(self):
        loaded_sdg_config = {
                    'settings': {'workers': 8, 'max_file_size_gb': 1, 'chunk_size': 10000},
                    'CustomGenerationValues': {
                        'custom_lists': {'dog_names': ['Hana', 'Youpie', 'Charlie', 'Lucy', 'Cooper', 'Luna', 'Rocky', 'Daisy', 'Buddy', 'Molly'],
                                        'dog_breeds': ['Jindo', 'Labrador', 'German Shepherd', 'Golden Retriever', 'Bulldog',
                                                       'Poodle', 'Beagle', 'Rottweiler', 'Boxer', 'Dachshund', 'Chihuahua'],
                                        'treats': ['cookies', 'pup_cup', 'jerky'], 'license_plates': ['WOOF101', 'BARKATAMZN'],
                                        'tips': ['biscuits', 'cash'],
                                        'skills': ['sniffing', 'squirrel_chasing', 'bite_tail', 'smile'],
                                        'vehicle_types': ['sedan', 'suv', 'truck'], 'vehicle_makes': ['toyta', 'honda', 'nissan'],
                                        'vehicle_models': ['rav4', 'accord', 'murano'], 'vehicle_years': [2012, 2015, 2019],
                                        'vehicle_colors': ['white', 'red', 'blue', 'black', 'silver'], 'account_status': ['active', 'inactive']},
                        'custom_providers': ['NumericString', 'MultipleChoices']
                    }
                }


        sdg_config = SDGConfig(**loaded_sdg_config)
        return sdg_config

    @pytest.fixture
    def mock_custom_module(self):
        mock_module = MagicMock
        mock_module.generate_synthetic_document = MagicMock(return_value={'synthetic_field': 'synthetic_value'})

        return mock_module

    @pytest.fixture
    def mock_dask_client(self):
        mock_scheduler_info = {'id': '123456789',
                                'services': {},
                                'type': 'Scheduler',
                                'workers': {'127.0.0.1:12345': {'active': 0,
                                                                'last-seen': 123412415.1234124,
                                                                'name': '127.0.0.1:12345',
                                                                'services': {},
                                                                'stored': 0,
                                                                'time-delay': 0.12390819587}}}

        mock_dask_client = MagicMock()
        mock_dask_client.scheduler_info = MagicMock(return_value=mock_scheduler_info)

        return mock_dask_client

    @pytest.fixture
    def setup_custom_strategy(self, setup_sdg_metadata, mock_sdg_config, mock_custom_module):
        custom_strategy = MagicMock()
        custom_strategy.generate_test_document.return_value = {'name': 'Shanks'}

        return custom_strategy

    @pytest.fixture
    def setup_custom_sdg(self, setup_sdg_metadata, mock_sdg_config, setup_custom_strategy):
        return SyntheticDataGenerator(setup_sdg_metadata, mock_sdg_config, setup_custom_strategy)

    # Patch how it's used in SDG
    @patch('osbenchmark.synthetic_data_generator.synthetic_data_generator.get_client')
    def test_generate_seeds_for_workers(self, mock_get_client, setup_custom_sdg, mock_dask_client):
        mock_get_client.return_value = mock_dask_client
        sdg = setup_custom_sdg

        result = sdg.generate_seeds_for_workers()
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], int)
        assert result[0] != 12345

    def test_generate_test_document(self, setup_custom_sdg):
        sdg = setup_custom_sdg
        result = sdg.generate_test_document()

        sdg.strategy.generate_test_document.assert_called_once()
        assert result == {'name': 'Shanks'}

    def test_generate_dataset(self):
        pass

class TestSyntheticDataGeneratorWithMappingStrategy:

    @pytest.fixture
    def setup_sdg_metadata(self):
        return SyntheticDataGeneratorMetadata(
            index_name="test-custom-module-sdg",
            index_mappings_path="path/to/mappings",
            custom_config_path="/path/to/config",
            custom_module_path="/path/to/module",
            output_path="/path/to/output",
            total_size_gb=10
        )

    @pytest.fixture
    def mock_sdg_config(self):
        loaded_sdg_config = {
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

        sdg_config = SDGConfig(**loaded_sdg_config)
        return sdg_config

    @pytest.fixture
    def mock_custom_module(self):
        mock_module = MagicMock
        mock_module.generate_synthetic_document = MagicMock(return_value={'synthetic_field': 'synthetic_value'})

        return mock_module

    @pytest.fixture
    def mock_dask_client(self):
        mock_scheduler_info = {'id': '123456789',
                                'services': {},
                                'type': 'Scheduler',
                                'workers': {'127.0.0.1:12345': {'active': 0,
                                                                'last-seen': 123412415.1234124,
                                                                'name': '127.0.0.1:12345',
                                                                'services': {},
                                                                'stored': 0,
                                                                'time-delay': 0.12390819587}}}

        mock_dask_client = MagicMock()
        mock_dask_client.scheduler_info = MagicMock(return_value=mock_scheduler_info)

        return mock_dask_client

    @pytest.fixture
    def setup_mapping_strategy(self, setup_sdg_metadata, mock_sdg_config, mock_custom_module):
        mapping_strategy = MagicMock()
        mapping_strategy.generate_test_document.return_value = {'name': 'Shanks'}

        return mapping_strategy

    @pytest.fixture
    def setup_custom_sdg(self, setup_sdg_metadata, mock_sdg_config, setup_mapping_strategy):
        return SyntheticDataGenerator(setup_sdg_metadata, mock_sdg_config, setup_mapping_strategy)

    # Patch how it's used in SDG
    @patch('osbenchmark.synthetic_data_generator.synthetic_data_generator.get_client')
    def test_generate_seeds_for_workers(self, mock_get_client, setup_custom_sdg, mock_dask_client):
        mock_get_client.return_value = mock_dask_client
        sdg = setup_custom_sdg

        result = sdg.generate_seeds_for_workers()
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], int)
        assert result[0] != 12345

    def test_generate_test_document(self, setup_custom_sdg):
        sdg = setup_custom_sdg
        result = sdg.generate_test_document()

        sdg.strategy.generate_test_document.assert_called_once()
        assert result == {'name': 'Shanks'}
