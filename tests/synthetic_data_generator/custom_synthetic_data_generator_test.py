# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from unittest.mock import MagicMock
import pytest

from mimesis.providers.base import BaseProvider

from osbenchmark.synthetic_data_generator.custom_synthetic_data_generator import (
    generate_data_chunk,
    instantiate_all_providers,
    seed_providers,
    add_custom_providers,
    generate_test_document
)

class TestProviders:
    def test_instantiate_all_providers(self):
        providers = instantiate_all_providers({})
        assert 'generic' in providers
        assert 'random' in providers

    def test_seed_providers(self):
        providers = instantiate_all_providers({})
        seeded_providers = seed_providers(providers, seed=42)

        assert providers == seeded_providers

    def test_add_custom_providers(self):
        # Custom Synthetic Data Generator uses Mimesis under the hood.
        # This is a custom provider that generates a specific string
        class GenerateCustomValue(BaseProvider):
            class Meta:
                name = "custom"

            def get_value(self):
                return "custom_value"

        generic = instantiate_all_providers({})['generic']
        # Register the custom provider we just created
        updated_generic = add_custom_providers(generic, {"custom": GenerateCustomValue})

        assert hasattr(updated_generic, 'custom')

class TestCustomSyntheticDataGeneration:
    @pytest.fixture
    def mock_generate_fake_document_callable(self):
        # This is a mock callable that returns a fake document
        return MagicMock(return_value={"fake_field": "fake_value"})

    @pytest.fixture
    def custom_config(self):
        return {
            "providers": {},
            "lists": {}
        }

    def test_generate_test_document(self, mock_generate_fake_document_callable, custom_config):
        document = generate_test_document(
            mock_generate_fake_document_callable,
            custom_config["lists"],
            custom_config["providers"]
        )

        mock_generate_fake_document_callable.assert_called_once()
        assert document == {"fake_field": "fake_value"}

    def test_generate_data_chunk(self, mock_generate_fake_document_callable, custom_config):
        docs_per_chunk = 5
        documents = generate_data_chunk(
            mock_generate_fake_document_callable,
            docs_per_chunk,
            custom_config["lists"],
            custom_config["providers"],
            seed=27
        )

        assert len(documents) == docs_per_chunk
        assert mock_generate_fake_document_callable.call_count == docs_per_chunk
