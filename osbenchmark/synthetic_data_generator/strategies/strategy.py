# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

from abc import ABC, abstractmethod
from typing import Optional, Callable

from dask.distributed import Client


from osbenchmark.synthetic_data_generator.types import SyntheticDataGeneratorMetadata

class DataGenerationStrategy:

    @abstractmethod
    def generate_data_chunks_across_workers(self, dask_client: Client, docs_per_chunk: int, seeds: list ) -> list:
        """
        Submit requests to generate data chunks across Dask workers

        returns: Dask Futures
        """

    @abstractmethod
    def generate_data_chunk_from_worker(self, logic_function: Callable, docs_per_chunk: int, seed: Optional[int]) -> list:
        """
        Generate chunk of docs with data generation logic for Dask worker

        returns: list of documents
        """
        pass

    @abstractmethod
    def generate_test_document(self) -> dict:
        """Generate test document from data generation logic"""
        pass

    @abstractmethod
    def calculate_avg_doc_size(self) -> int:
        """Calculates avg doc size based on data generation logic"""
        pass
