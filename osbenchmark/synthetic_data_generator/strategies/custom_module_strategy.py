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

from dask.distributed import Client
from mimesis import Generic
from mimesis.locales import Locale
from mimesis.random import Random
from mimesis.providers.base import BaseProvider

from osbenchmark import exceptions
from osbenchmark.synthetic_data_generator.strategies import DataGenerationStrategy
from osbenchmark.synthetic_data_generator.models import SyntheticDataGeneratorMetadata, SDGConfig

class CustomModuleStrategy(DataGenerationStrategy):
    def __init__(self, sdg_metadata: SyntheticDataGeneratorMetadata,  sdg_config: SDGConfig, custom_module: ModuleType) -> None:
        self.sdg_metadata = sdg_metadata
        self.sdg_config = sdg_config
        self.custom_module = custom_module
        self.logger = logging.getLogger(__name__)

        if not hasattr(self.custom_module, 'generate_synthetic_document'):
            msg = f"Custom module at [{self.sdg_metadata.custom_module_path}] does not define a function called generate_synthetic_document(). Ensure that this method is defined."
            raise exceptions.ConfigError(msg)

        # Fetch settings and custom module components from sdg-config.yml
        if self.sdg_config.CustomGenerationValues is None:
            self.custom_lists = {}
            self.custom_providers = {}
        else:
            try:
                self.custom_lists = self.sdg_config.CustomGenerationValues.custom_lists or {}
                provider_names = self.sdg_config.CustomGenerationValues.custom_providers or []
                self.custom_providers = {
                    name: getattr(self.custom_module, name) for name in provider_names
                }
            except AttributeError as e:
                msg = f"Error when setting up custom lists and custom providers: {e}"
                raise exceptions.ConfigError(msg)
            except TypeError:
                msg = "Synthetic Data Generator Config has custom_lists and custom_providers pointing to null values. Either populate or remove."


    # pylint: disable=arguments-differ
    def generate_data_chunks_across_workers(self, dask_client: Client, docs_per_chunk: int, seeds: list ) -> list:
        """
        Submits workers to generate data chunks and returns Dask futures

        Returns: list of Dask Futures
        """
        return [dask_client.submit(
            self.generate_data_chunk_from_worker, self.custom_module.generate_synthetic_document,
            docs_per_chunk, seed) for seed in seeds]

    # pylint: disable=arguments-renamed
    def generate_data_chunk_from_worker(self, generate_synthetic_document: Callable, docs_per_chunk: int, seed: Optional[int]) -> list:
        """
        This method is submitted to Dask worker and can be thought of as the worker performing a job, which is calling the
        custom module's generate_synthetic_document() function to generate documents.
        The worker will call the function N number of times (determined by docs_per_chunk in sdg-config.yml)
        to generate N docs of data before returning results.

        :param generate_synthetic_document: This is the callable that the user must define in their module.
            The callable should be named 'generate_synthetic_document()'
        :param docs_per_chunk: The number of documents the worker needs to generate before returning them in a list
        :instance variable custom_lists (optional): These are custom lists that the user_defined_function uses to generate random values
        :instance variable custom_providers (optional): These are custom providers (written in Mimesis or Faker) that generate data in a specific way.
            Users define this in the same file as generate_synthetic_document() function. Custom providers must extend from the BaseProviders class.

        :returns List of documents to be written or published to a source (e.g. disk or S3 bucket)
        """
        providers = self._instantiate_all_providers(self.custom_providers)
        seeded_providers = self._seed_providers(providers, seed)

        return [generate_synthetic_document(providers=seeded_providers, **self.custom_lists) for _ in range(docs_per_chunk)]

    def generate_test_document(self):
        providers = self._instantiate_all_providers(self.custom_providers)
        providers = self._seed_providers(providers)

        try:
            document = self.custom_module.generate_synthetic_document(providers=providers, **self.custom_lists)
        except AttributeError as e:
            msg = "Encountered AttributeError when setting up custom_providers and custom_lists. " + \
                    "It seems that your module might be using custom_lists and custom_providers." + \
                    f"Please ensure you have provided a custom config with custom_providers and custom_lists: {e}"
            raise exceptions.ConfigError(msg)
        return document

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
        '''
        Generic Mimesis uses reseed method while non-generic Mimesis (like Random) uses seed method. Both lead to the same effect.
        '''
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
