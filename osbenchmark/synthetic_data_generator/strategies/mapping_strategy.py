# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import logging
from typing import Optional, Callable, Dict, Any, Generator
import random
import datetime
import uuid

from dask.distributed import Client
from mimesis import Generic
from mimesis.locales import Locale
from mimesis.random import Random

from osbenchmark import exceptions
from osbenchmark.synthetic_data_generator.strategies import DataGenerationStrategy
from osbenchmark.synthetic_data_generator.models import SyntheticDataGeneratorMetadata, SDGConfig, MappingGenerationValuesConfig
from osbenchmark.synthetic_data_generator.timeseries_partitioner import TimeSeriesPartitioner

class MappingStrategy(DataGenerationStrategy):
    def __init__(self, sdg_metadata: SyntheticDataGeneratorMetadata,  sdg_config: SDGConfig, index_mapping: dict) -> None:
        self.sdg_metadata = sdg_metadata
        self.sdg_config = sdg_config # Optional YAML-based config for value constraints
        self.index_mapping = index_mapping # OpenSearch Mapping
        self.mapping_generation_values =  (self.sdg_config.MappingGenerationValues or {}) if self.sdg_config else {}

        self.logger = logging.getLogger(__name__)

    def generate_data_chunks_across_workers(self, dask_client: Client, docs_per_chunk: int, seeds: list, timeseries_enabled: dict, timeseries_windows: list) -> list:
        """
        Submits workers to generate data chunks and returns Dask futures

        Returns: list of Dask Futures
        """
        if timeseries_enabled and timeseries_windows:
            futures = []
            # pylint: disable=consider-using-enumerate
            for _ in range(len(seeds)):
                seed = seeds[_]
                window = timeseries_windows[_]
                future = dask_client.submit(
                    self.generate_data_chunk_from_worker,
                    docs_per_chunk, seed, timeseries_enabled, window
                )

                futures.append(future)

        else:
            futures = [dask_client.submit(self.generate_data_chunk_from_worker, docs_per_chunk, seed) for seed in seeds]

        return futures

    # pylint: disable=arguments-differ
    def generate_data_chunk_from_worker(self, docs_per_chunk: int, seed: Optional[int], timeseries_enabled: dict = None, timeseries_window: set = None) -> list:
        """
        This method is submitted to Dask worker and can be thought of as the worker performing a job, which is calling the
        MappingConverter's static method generate_synthetic_document() function to generate documents.
        The worker will call the function N number of times to generate N docs of data before returning results.

        Note: This method reconstructs the MappingConverter because Dask coordinator requires serializing and deserializing objects
        when passing them to a worker. Generates the generate_synthetic_document, which gets invoked N number of times
        before returning a list of documents.

        Returns: List of generated documents.
        """
        # Initialize mapping generation values (params from sdg-config.yml) given to worker
        mapping_generator_logic = MappingConverter(self.mapping_generation_values, seed)
        mappings_with_generators = mapping_generator_logic.transform_mapping_to_generators(self.index_mapping)

        if timeseries_enabled and timeseries_enabled.timeseries_field:
            synthetic_docs = []
            datetimestamps: Generator = TimeSeriesPartitioner.generate_datetimestamps_from_window(
                window=timeseries_window, frequency=timeseries_enabled.timeseries_frequency, format=timeseries_enabled.timeseries_format
                )
            for datetimestamp in datetimestamps:
                document = MappingConverter.generate_synthetic_document(mappings_with_generators)
                try:
                    document[timeseries_enabled.timeseries_field] = datetimestamp
                    synthetic_docs.append(document)

                except Exception as e:
                    raise exceptions.DataError(f"Encountered problem when inserting datetimestamps for timeseries data being generated: {e}")

            return synthetic_docs


        documents = [MappingConverter.generate_synthetic_document(mappings_with_generators) for _ in range(docs_per_chunk)]

        return documents

    def generate_test_document(self, timeseries_enabled: dict = None, timeseries_window: set = None):
        mapping_converter = MappingConverter(self.mapping_generation_values)
        converted_mappings = mapping_converter.transform_mapping_to_generators(self.index_mapping)

        document = MappingConverter.generate_synthetic_document(transformed_mapping=converted_mappings)
        if timeseries_enabled and timeseries_enabled.timeseries_field:
            datetimestamps: Generator = TimeSeriesPartitioner.generate_datetimestamps_from_window(
                window=timeseries_window, frequency=timeseries_enabled.timeseries_frequency, format=timeseries_enabled.timeseries_format
                )
            for datetimestamp in datetimestamps:
                document[timeseries_enabled.timeseries_field] = datetimestamp

        return document


class MappingConverter:
    def __init__(self, mapping_generation_values=None, seed=1):
        self.logger = logging.getLogger(__name__)
        self.mapping_config = mapping_generation_values if mapping_generation_values else {}

        self.generic = Generic(locale=Locale.EN)
        self.random = Random()

        self.generic.reseed(seed)
        self.random.seed(seed)
        random.seed(seed)

        # seed these
        # TODO: Should apply all of these: https://docs.opensearch.org/latest/mappings/supported-field-types/index/
        self.type_generators = {
            "text": self.generate_text,
            "keyword": self.generate_keyword,
            "long": self.generate_long,
            "integer": self.generate_integer,
            "short": self.generate_short,
            "byte": self.generate_byte,
            "double": self.generate_double,
            "float": self.generate_float,
            "boolean": self.generate_boolean,
            "date": self.generate_date,
            "ip": self.generate_ip,
            "object": self.generate_object,
            "nested": self.generate_nested,
            "geo_point": self.generate_geo_point,
            "knn_vector": self.generate_knn_vector,
            "sparse_vector": self.generate_sparse_vector,
        }

    @staticmethod
    def generate_synthetic_document(transformed_mapping: Dict[str, Callable]) -> Dict[str, Any]:
        """
        Generate a document using the generator functions

        Args:
            transformed_mapping: Dictionary of generator functions

        Returns:
            document containing lambdas that can be invoked to generate data
        """
        document = {}
        for field_name, generator in transformed_mapping.items():
            document[field_name] = generator()

        return document

    def generate_text(self, field_def: Dict[str, Any],  **params) -> str:
        choices = params.get('must_include', None)
        analyzer = field_def.get("analyzer", "standard")

        #TODO: Need to support other analyzers
        text = ""
        if choices:
            term = random.choice(choices)
            text += f"{term} "
        if analyzer == "keyword":
            text += f"keyword_{uuid.uuid4().hex[:8]}"
            return text

        text += f"Sample text for {random.randint(1, 100)}"
        return text


    def generate_keyword(self, field_def: Dict[str, Any], **params) -> str:
        choices = params.get('choices', None)
        if choices:
            keyword = random.choice(choices)
            return keyword
        else:
            return f"key_{uuid.uuid4().hex[:8]}"

    def generate_long(self, field_def: Dict[str, Any], **params) -> int:
        min = params.get('min', -(2**63 - 1))
        max = params.get('max', (2**63 - 1))
        return random.randint(min, max)

    def generate_integer(self, field_def: Dict[str, Any], **params) -> int:
        min = params.get('min', -2147483648)
        max = params.get('max', 2147483647)

        return random.randint(min, max)

    def generate_short(self, field_def: Dict[str, Any], **params) -> int:
        min = params.get('min', -32768)
        max = params.get('max', 32767)
        return random.randint(min, max)

    def generate_byte(self, field_def: Dict[str, Any], **params) -> int:
        min = params.get('min', -128)
        max = params.get('max', 127)
        return random.randint(min, max)

    def generate_double(self, field_def: Dict[str, Any], **params) -> float:
        min = params.get('min', -1e9)
        max = params.get('max', 1e9)
        return random.uniform(min, max)

    def generate_float(self, field_def: Dict[str, Any], **params) -> float:
        min = params.get('min', 0)
        max = params.get('max', 1000)
        decimal_places = params.get('round', 2)

        float_value = random.uniform(min, max)
        return round(float_value , decimal_places)

    def generate_boolean(self, field_def: Dict[str, Any], **params) -> bool:
        return random.choice([True, False])

    def generate_date(self, field_def: Dict[str, Any], **params,) -> str:
        # TODO Need to handle actual format values
        # If field definition includes format, then use it.
        date_format = field_def.get("format", "yyyy-mm-dd")

        # Get a date range (from mapping config or default) from config
        # If user specified other format then default to config format
        date_format = params.get("format", date_format)
        start_date = params.get("start_date", "2000-01-01")
        end_date = params.get("end_date", "2030-12-31")

        start_dt = datetime.datetime.fromisoformat(start_date)
        end_dt = datetime.datetime.fromisoformat(end_date)
        random_date = start_dt + datetime.timedelta(
            days=random.randint(0, (end_dt - start_dt).days)
        )

        # Apply formatting
        if date_format == "yyyy-mm-dd":
            return random_date.strftime("%Y-%m-%d")
        elif date_format == "yyyy-mm-dd'T'HH:mm:ssZ":
            return random_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        return random_date.isoformat()  # Default ISO format

    def generate_ip(self, field_def: Dict[str, Any], **params) -> str:
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}"

    def generate_geo_point(self, field_def: Dict[str, Any], **params) -> Dict[str, float]:
        return {
            "lat": random.uniform(-90, 90),
            "lon": random.uniform(-180, 180)
        }

    def generate_object(self, field_def: Dict[str, Any], **params) -> Dict[str, Any]:
        # This will be replaced by the nested fields generator
        return {}

    def generate_nested(self, field_def: Dict[str, Any], **params) -> list:
        # Will be replaced by a list of nested objects
        return []

    def generate_knn_vector(self, field_def: Dict[str, Any], **params) -> list:
        """
        Generate dense vector embeddings for knn_vector field type.
        Supports both random generation and sample-based generation with noise for realistic clustering

        Args:
            field_def: Field definition from mapping
            **params: Optional parameters:
                dimension: Vector dimensions. Can be retrieved from field_ef (default: 128)
                sample_vectors: List of base vectors to add noise to. Helps with realistic clustering.
                               Without sample_vectors, OSB generates uniform random vectors between -1.0 and 1.0
                noise_factor: Standard deviation (gaussian) or range (uniform) of noise (default: 0.1)
                             Lower values (0.01-0.05) create tight clusters.
                             Higher values (0.2-0.5) create diverse distributions.
                distribution_type: Type of noise distribution (default: "gaussian").
                              "gaussian": Normal distribution, realistic with outliers
                              "uniform": Bounded distribution, predictable variation
                normalize: Whether to normalize the vector after generation (default: False)
                            Set to True when using cosinesimil space_type in OpenSearch.
                            Normalized vectors have magnitude = 1.0.

        Returns:
            List of floats representing the dense vector.
            When using sample_vectors, creates a realistic variation around sampled clusters provided.
            Without sample_vectors, it uses random uniform values between -1.0 and 1.0.
        """

        dims = field_def.get("dimension", params.get("dimension", 128))
        sample_vectors = params.get("sample_vectors", None)

        if sample_vectors:
            noise_factor = params.get("noise_factor", 0.1)
            distribution_type = params.get("distribution_type", "gaussian")
            normalize = params.get("normalize", False)

            # Pick random sample vector
            base_vector = random.choice(sample_vectors)

            # Generate noise based on distribution type
            if distribution_type == "gaussian":
                noise = [random.gauss(0, noise_factor) for _ in range(dims)]
            else:  # uniform
                noise = [random.uniform(-noise_factor, noise_factor) for _ in range(dims)]

            # Add noise to base vector
            vector = [base_vector[i] + noise[i] for i in range(dims)]

            # Normalize if requested
            if normalize:
                magnitude = sum(x**2 for x in vector) ** 0.5
                if magnitude > 0:
                    vector = [x / magnitude for x in vector]

            return vector

        else:
            # Fallback to random generation with each dimension being between -1 and 1
            return [random.uniform(-1.0, 1.0) for _ in range(dims)]

    def generate_sparse_vector(self, field_def: Dict[str, Any], **params) -> Dict[str, float]:
        """
        Generate sparse vector as token_id -> weight pairs for sparse_vector field type.

        Args:
            field_def: Field definition from mapping
            **params: The following are optional parameters:
                num_tokens: Number of token-weight pairs (default: 10)
                min_weight: Minimum weight value (default: 0.01)
                max_weight: Maximum weight value (default: 1.0)
                token_id_start: Starting token ID (default: 1000)
                token_id_step: Step between token IDs (default: 100)

        Returns:
            Dict of token_id -> weight pairs with positive float values
        """
        num_tokens = params.get('num_tokens', 10)
        min_weight = params.get('min_weight', 0.01)
        max_weight = params.get('max_weight', 1.0)
        token_id_start = params.get('token_id_start', 1000)
        token_id_step = params.get('token_id_step', 100)

        sparse_vector = {}
        for i in range(num_tokens):
            token_id = str(token_id_start + (i * token_id_step))
            weight = random.uniform(min_weight, max_weight)
            sparse_vector[token_id] = round(weight, 4) # imitate real neural sparse search models like Splade and DeepImpact

        return sparse_vector

    def transform_mapping_to_generators(self, mapping_dict: Dict[str, Any], field_path_prefix="") -> Dict[str, Callable[[], Any]]:
        """
        Transforms an OpenSearch mapping into a dictionary of field names mapped to generator functions.

        Args:
            mapping_dict: OpenSearch mapping provided by user
            field_path_prefix: Path leading up to current field. Useful for tracking nested fields

        Returns:
            dictionary of field names mapped to generator functions
        """
        # Initialize transformed_mappings
        transformed_mapping = {}

        # Extract configuration settings (both default generators and field overrides) from config user provided
        # Convert the sdg_config's MappingGenerationValues section into a dictionary and access the overrides
        if isinstance(self.mapping_config, MappingGenerationValuesConfig):
            config = self.mapping_config.model_dump()
        else:
            config = self.mapping_config

        generator_overrides = config.get("generator_overrides", {})
        field_overrides = config.get("field_overrides", {})

        try:
            if "mappings" in mapping_dict:
                properties = mapping_dict["mappings"].get("properties", {})
            else:
                properties = mapping_dict.get("properties", mapping_dict)
        except KeyError:
            raise exceptions.MappingsError("OpenSearch mappings provided are invalid. Please ensure it includes 'mappings' or 'properties' fields.")

        # Iterate through all the properties in the index mapping
        for field_name, field_def in properties.items():
            # print("generator dict: ", transformed_mapping)
            field_type = field_def.get("type")
            current_field_path = f"{field_path_prefix}.{field_name}" if field_path_prefix else field_name

            # Fields with no types provided but have properties field are considered type object by default
            # NOTE: We do not care for multifields. This is more of an ingestion technique
            # where OpenSearch ingests the same field in different ways.
            # It does not change the data generated.
            if field_type is None and "properties" in field_def:
                field_type = "object"

            if field_type in {"object", "nested"} and "properties" in field_def:
                nested_generator = self.transform_mapping_to_generators(mapping_dict=field_def, field_path_prefix=current_field_path)
                if field_type == "object":
                    transformed_mapping[field_name] = lambda f=field_def, ng=nested_generator: self._generate_obj(f, ng)
                else:
                    transformed_mapping[field_name] = lambda f=field_def, ng=nested_generator: self._generate_nested_array(f, ng)
                continue

            if current_field_path in field_overrides:
                override = field_overrides[current_field_path]
                gen_name = override.get("generator")
                gen_func = getattr(self, gen_name, None)
                if gen_func:
                    params = override.get("params", {})
                    transformed_mapping[field_name] = lambda f=field_def, gen=gen_func, p=params: gen(f, **p)
                else:
                    self.logger.info("Issue with sdg-config.yml: override for field [%s] specifies non-existent data generator [%s]", current_field_path, gen_name)
                    msg = f"Issue with sdg-config.yml: override for field [{current_field_path}] specifies non-existent data generator [{gen_name}]"
                    raise exceptions.ConfigError(msg)
            else:
                # Check if default_generators has overrides for all instances of a type of generator
                generator_override_params = generator_overrides.get(field_type, {})
                # A dummy lambda must be returned because it runs into TypeError when it's a callable.
                # Need to maintain interface compatability because all the self.type_generators use fields and **kwargs
                generator_func = self.type_generators.get(field_type, lambda field, **_: "unknown_type")

                transformed_mapping[field_name] = lambda f=field_def, gen=generator_func, p=generator_override_params: gen(f, **p)


        return transformed_mapping


    def _generate_obj(self, field_def: Dict[str, Any], nested_generators: Dict[str, Callable]) -> Dict[str, Any]:
        """Generate an object using nested generators"""
        result = {}
        for field_name, generator in nested_generators.items():
            result[field_name] = generator()
        return result

    def _generate_nested_array(self, field_def: Dict[str, Any], nested_generators: Dict[str, Callable], min_items=1, max_items=3) -> list:
        """Generate nested array of objects"""
        count = random.randint(min_items, max_items)
        result = []
        for _ in range(count):
            obj = {}
            for field_name, generator in nested_generators.items():
                obj[field_name] = generator()
            result.append(obj)
        return result
