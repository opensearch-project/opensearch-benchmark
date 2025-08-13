# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
import logging
import time
import hashlib

from dask.distributed import Client, get_client, as_completed
from tqdm import tqdm

from osbenchmark.utils import console
from osbenchmark.synthetic_data_generator import helpers
from osbenchmark.synthetic_data_generator.strategies import DataGenerationStrategy
from osbenchmark.synthetic_data_generator.models import SyntheticDataGeneratorMetadata, SDGConfig, GB_TO_BYTES

class SyntheticDataGenerator:
    def __init__(self, sdg_metadata: SyntheticDataGeneratorMetadata, sdg_config: SDGConfig, strategy: DataGenerationStrategy) -> None:
        self.sdg_metadata = sdg_metadata
        self.sdg_config = sdg_config
        self.strategy = strategy

        self.logger = logging.getLogger(__name__)

    def generate_seeds_for_workers(self, regenerate=False):
        # This adds latency so might consider deprecating this
        client = get_client()
        workers = client.scheduler_info()['workers']

        seeds = []
        for worker_id in workers.keys():
            hash_object = hashlib.md5(worker_id.encode())

            if regenerate:
                # Add current timestamp to each hash to improve uniqueness
                timestamp = str(time.time()).encode()
                hash_object.update(timestamp)

            hash_hex = hash_object.hexdigest()

            seed = int(hash_hex[:8], 16)
            seeds.append(seed)

        return seeds

    def generate_test_document(self):
        return self.strategy.generate_test_document()

    def generate_dataset(self):
        """
        Core logic in generating synthetic data. Can use different strategies
        """
        max_file_size_bytes: int = self.sdg_config.settings.max_file_size_gb * GB_TO_BYTES
        total_size_bytes: int = self.sdg_metadata.total_size_gb * GB_TO_BYTES
        docs_per_chunk: int = self.sdg_config.settings.docs_per_chunk

        avg_document_size = helpers.calculate_avg_doc_size(strategy=self.strategy)

        current_size = 0
        docs_written = 0
        file_counter = 0

        generated_dataset_details = []

        helpers.check_for_existing_files(self.sdg_metadata.output_path, self.sdg_metadata.index_name)

        workers: int = self.sdg_config.settings.workers
        dask_client = Client(n_workers=workers, threads_per_worker=1)  # We keep it to 1 thread because generating random data is CPU intensive
        self.logger.info("Number of workers to use: [%s]", workers)

        console.println(f"[NOTE] ✨ Dashboard link to monitor processes and task streams: [{dask_client.dashboard_link}]")
        console.println("[NOTE] ✨ For users who are running generation on a virtual machine, consider SSH port forwarding (tunneling) to localhost to view dashboard.")
        console.println("[NOTE] Example of localhost command for SSH port forwarding (tunneling) from an AWS EC2 instance: ")
        console.println("ssh -i <PEM filepath> -N -L localhost:8787:localhost:8787 ec2-user@<DNS>")
        console.println("")

        self.logger.info("Average document size in bytes: [%s]", avg_document_size)
        self.logger.info("Chunk size: [%s] docs", docs_per_chunk)
        self.logger.info("Total GB to generate: [%s]", self.sdg_metadata.total_size_gb)
        self.logger.info("Max file size in GB: [%s]", self.sdg_config.settings.max_file_size_gb)

        console.println(f"Total GB to generate: [{self.sdg_metadata.total_size_gb}]\n"
                        f"Average document size in bytes: [{avg_document_size}]\n"
                        f"Max file size in GB: [{self.sdg_config.settings.max_file_size_gb}]\n")

        start_time = time.time()
        with tqdm(total=total_size_bytes,
                    unit='B',
                    unit_scale=True,
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as progress_bar:

            helpers.setup_custom_tqdm_formatting(progress_bar)
            while current_size < total_size_bytes:
                file_path = os.path.join(self.sdg_metadata.output_path, f"{self.sdg_metadata.index_name}_{file_counter}.json")
                file_size = 0
                docs_written = 0

                while file_size < max_file_size_bytes:
                    generation_start_time = time.time()
                    # Generate data across all workers
                    seeds = self.generate_seeds_for_workers(regenerate=True)
                    self.logger.info("Using seeds: %s", seeds)

                    futures = self.strategy.generate_data_chunks_across_workers(dask_client, docs_per_chunk, seeds)

                    writing_start_time = time.time()
                    for _, data in as_completed(futures, with_results=True):
                        self.logger.info("Future [%s] completed.", _)
                        docs_written_from_chunk, written_bytes = helpers.write_chunk(data, file_path)
                        docs_written += docs_written_from_chunk
                        current_size += written_bytes
                        progress_bar.update(written_bytes)
                    writing_end_time = time.time()

                    generating_took_time = writing_start_time - generation_start_time
                    writing_took_time = writing_end_time - writing_start_time
                    self.logger.info("Generating took [%s] seconds", generating_took_time)
                    self.logger.info("Writing took [%s] seconds", writing_took_time)

                    file_size = os.path.getsize(file_path)
                    # If it exceeds the max file size, then append this to keep track of record
                    if file_size >= max_file_size_bytes:
                        file_name = os.path.basename(file_path)
                        generated_dataset_details.append({
                            "file_name": file_name,
                            "docs": docs_written,
                            "file_size_bytes": file_size
                        })
                        if current_size >= total_size_bytes:
                            break

                    if current_size >= total_size_bytes:
                        file_name = os.path.basename(file_path)
                        generated_dataset_details.append({
                            "file_name": file_name,
                            "docs": docs_written,
                            "file_size_bytes": file_size
                        })
                        break

                file_counter += 1

            end_time = time.time()
            total_time_to_generate_dataset = round(end_time - start_time)
            progress_bar.update(total_size_bytes - progress_bar.n)

            self.logger.info("Generated dataset in [%s] seconds. Dataset generation details: [%s]", total_time_to_generate_dataset, generated_dataset_details)

            return total_time_to_generate_dataset, generated_dataset_details
