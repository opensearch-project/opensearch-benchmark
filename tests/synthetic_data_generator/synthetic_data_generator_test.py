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
from osbenchmark.synthetic_data_generator.strategies import MappingStrategy, CustomModuleStrategy
from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata
from osbenchmark.synthetic_data_generator import helpers

class TestSyntheticDataGeneratorWithCustomStrategy:

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
        return {
            'providers': {},
            'lists': {}
        }

    @pytest.fixture
    def mock_custom_module(self):
        mock_module = MagicMock
        mock_module.generate_fake_document = MagicMock(return_value={'fake_field': 'fake_value'})

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
    def setup_custom_sdg(self, setup_sdg_metadata, mock_sdg_config, mock_dask_client, setup_custom_strategy):
        return SyntheticDataGenerator(setup_sdg_metadata, mock_sdg_config, mock_dask_client, setup_custom_strategy)

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
            total_size_gb=10,
            mode=None,
            checkpoint=None,
            blueprint=None,
            generators={}
        )

    @pytest.fixture
    def mock_sdg_config(self):
        return {
            'providers': {},
            'lists': {}
        }

    @pytest.fixture
    def mock_custom_module(self):
        mock_module = MagicMock
        mock_module.generate_fake_document = MagicMock(return_value={'fake_field': 'fake_value'})

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
    def setup_custom_sdg(self, setup_sdg_metadata, mock_sdg_config, mock_dask_client, setup_mapping_strategy):
        return SyntheticDataGenerator(setup_sdg_metadata, mock_sdg_config, mock_dask_client, setup_mapping_strategy)

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
