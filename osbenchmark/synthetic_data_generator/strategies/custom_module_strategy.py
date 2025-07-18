# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
from types import ModuleType
from typing import Optional, Callable
import time
import os
import hashlib
import importlib.util

from dask.distributed import Client, get_client, as_completed
from mimesis import Generic
from mimesis.locales import Locale
from mimesis.random import Random
from mimesis.providers.base import BaseProvider
from tqdm import tqdm

from osbenchmark.exceptions import ConfigError
from osbenchmark.synthetic_data_generator.strategies import DataGenerationStrategy
from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata
from osbenchmark.synthetic_data_generator.helpers import write_chunk, get_generation_settings, setup_custom_tqdm_formatting

class CustomModuleStrategy(DataGenerationStrategy):
    def __init__(self, sdg_metadata: SyntheticDataGeneratorMetadata,  sdg_config: dict, custom_module: ModuleType) -> None:
        self.sdg_metadata = sdg_metadata
        self.sdg_config = sdg_config
        self.custom_module = custom_module

        if not hasattr(self.custom_module, 'generate_fake_document'):
            msg = f"Custom module at [{self.sdg_metadata.custom_module_path}] does not define a function called generate_fake_document(). Ensure that this method is defined."
            raise ConfigError(msg)

        # Fetch settings and custom module components from sdg-config.yml
        custom_module_values = self.sdg_config.get('CustomGenerationValues', {})
        try:
            self.custom_lists = custom_module_values.get('custom_lists', {})
            self.custom_providers = {name: getattr(self.custom_module, name) for name in custom_module_values.get('custom_providers', [])}
        except TypeError:
            msg = "Synthetic Data Generator Config has custom_lists and custom_providers pointing to null values. Either populate or remove."
            raise ConfigError(msg)


        self.logger = logging.getLogger(__name__)

    def generate_data_chunks_across_workers(self, dask_client: Client, docs_per_chunk: int, seeds: list ) -> list:
        """
        Submits workers to generate data chunks and returns Dask futures

        Returns: list of Dask Futures
        """
        return [dask_client.submit(
            self.generate_data_chunk_from_worker, self.custom_module.generate_fake_document,
            docs_per_chunk, seed) for seed in seeds]


    def generate_data_chunk_from_worker(self, generate_fake_document: Callable, docs_per_chunk: int, seed: Optional[int]) -> list:
        """
        This method is submitted to Dask worker and can be thought of as the worker performing a job, which is calling the
        custom module's generate_fake_document() function to generate documents.
        The worker will call the function N number of times to generate N docs of data before returning results.

        :param generate_fake_document: This is the callable that the user must define in their module.
            The callable should be named 'generate_fake_document()'
        :param docs_per_chunk: The number of documents the worker needs to generate before returning them in a list
        :instance variable custom_lists (optional): These are custom lists that the user_defined_function uses to generate random values
        :instance variable custom_providers (optional): These are custom providers (written in Mimesis or Faker) that generate data in a specific way.
            Users define this in the same file as generate_fake_document() function. Custom providers must extend from the BaseProviders class.

        :returns List of documents to be written or published to a source (e.g. disk or S3 bucket)
        """
        providers = self._instantiate_all_providers(self.custom_providers)
        seeded_providers = self._seed_providers(providers, seed)

        return [generate_fake_document(providers=seeded_providers, **self.custom_lists) for _ in range(docs_per_chunk)]

    def generate_test_document(self):
        providers = self._instantiate_all_providers(self.custom_providers)
        providers = self._seed_providers(providers)

        try:
            document = self.custom_module.generate_fake_document(providers=providers, **self.custom_lists)
        except AttributeError as e:
            msg = "Encountered AttributeError when setting up custom_providers and custom_lists. " + \
                    "It seems that your module might be using custom_lists and custom_providers." + \
                    f"Please ensure you have provided a custom config with custom_providers and custom_lists: {e}"
            raise ConfigError(msg)
        return document

    def calculate_avg_doc_size(self):
        # Didn't do pickle because this seems to be more accurate
        output = [self.generate_test_document()]
        write_chunk(output, '/tmp/test-size.json')

        size = os.path.getsize('/tmp/test-size.json')
        os.remove('/tmp/test-size.json')

        return size

    def _instantiate_all_providers(self, custom_providers):
        g = Generic(locale=Locale.DEFAULT)
        r = Random()

        if custom_providers:
            g = self._add_custom_providers(g, custom_providers)

        provider_instances = {
            'generic': g,
            'random': r
        }

        return provider_instances

    def _seed_providers(self, providers, seed=None):
        for key, provider_instance in providers.items():
            if key in ['generic']:
                provider_instance.reseed(seed)
            elif key in ['random']:
                provider_instance.seed(seed)

        return providers

    def _add_custom_providers(self, generic, custom_providers):
        for name, provider_class in custom_providers.items():
            if issubclass(provider_class, BaseProvider):
                generic.add_provider(provider_class)
            else:
                # If it's not a Mimesis provider, we'll add it as is
                setattr(generic, name, provider_class())
        return generic
