# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
from typing import Optional, Dict, List, Any, Union
import re

from pydantic import BaseModel, Field, field_validator

from osbenchmark.synthetic_data_generator.timeseries_partitioner import TimeSeriesPartitioner

GB_TO_BYTES = 1024 ** 3

class TimeSeriesConfig(BaseModel):
    timeseries_field: str
    timeseries_start_date: str
    timeseries_end_date: str
    timeseries_frequency: str
    timeseries_format: str

    # pylint: disable = no-self-argument
    @field_validator('timeseries_start_date', 'timeseries_end_date', 'timeseries_frequency', 'timeseries_format')
    def validate_string_fields(cls, v, info):
        """Validate that timeseries configuration fields are strings"""
        if not isinstance(v, str):
            field_name = info.field_name.replace('_', ' ').title()
            raise ValueError(f"{field_name} requires a string value. Value {v} is not valid.")

        # Additional validation for frequency and format fields
        if info.field_name == 'timeseries_frequency':
            if v not in TimeSeriesPartitioner.AVAILABLE_FREQUENCIES:
                raise ValueError(f"Timeseries frequency {v} is not a valid value. Valid values are {TimeSeriesPartitioner.AVAILABLE_FREQUENCIES}")

        if info.field_name == 'timeseries_format':
            if v not in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
                raise ValueError(f"Timeseries format {v} is not a valid value. Valid values are {TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS}")

        return v

    # pylint: disable = no-self-argument
    @field_validator('timeseries_field')
    def validate_timeseries_field(cls, v):
        if not v or not v.strip():
            raise ValueError("timeseries_field cannot be empty")

        # Validate field name format
        # OpenSearch field names must start with a letter and contain only alphanumeric, underscores, and periods
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_.]*$', v):
            raise ValueError(
                f"Invalid timeseries_field '{v}'. Field names must start with a letter "
                "and contain only alphanumeric characters, underscores, and periods."
            )

        return v

class SettingsConfig(BaseModel):
    workers: Optional[int] = Field(default_factory=os.cpu_count) # Number of workers recommended to not exceed CPU count
    max_file_size_gb: Optional[int] = 40                         # Default because some CloudProviders limit the size of files stored
    docs_per_chunk: Optional[int] = 10000                        # Default based on testing
    filename_suffix_begins_at: Optional[int] = 0                 # Start at suffix 0
    timeseries_enabled: Optional[TimeSeriesConfig] = None

    # pylint: disable = no-self-argument
    @field_validator('workers', 'max_file_size_gb', 'docs_per_chunk')
    def validate_values_are_positive_integers(cls, v):
        if v is not None and v <= 0:
            raise ValueError(f"Value '{v}' in Settings portion must be a positive integer.")

        return v

class CustomGenerationValuesConfig(BaseModel):
    custom_lists: Optional[Dict[str, List[Any]]] = None
    custom_providers: Optional[List[Any]] = None

    # pylint: disable = no-self-argument
    @field_validator('custom_lists')
    def validate_custom_lists(cls, v):
        if v is not None:
            for key, value in v.items():
                if not isinstance(key, str):
                    raise ValueError(f"All keys within custom_lists of CustomGenerationValues section must be strings. '{key}' is not a string")
                if not isinstance(value, list):
                    raise ValueError(f"Value for key '{key}' must be a list.")
        return v

class GeneratorParams(BaseModel):
    # Integer / Long Params
    min: Optional[Union[int, float]] = None
    max: Optional[Union[int, float]] = None

    # Date Params
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    format: Optional[str] = None

    # Text / Keywords Params
    must_include: Optional[List[str]] = None
    choices: Optional[List[str]] = None
    min_words: Optional[int] = None
    max_words: Optional[int] = None

    # Float / Double Params
    precision: Optional[int] = None

    # knn_vector Params
    dimension: Optional[int] = None
    sample_vectors: Optional[List[List[float]]] = None
    noise_factor: Optional[float] = None
    distribution_type: Optional[str] = None
    normalize: Optional[bool] = None

    # sparse_vector Params
    num_tokens: Optional[int] = None
    min_weight: Optional[float] = None
    max_weight: Optional[float] = None
    token_id_start: Optional[int] = None
    token_id_step: Optional[int] = None

    class Config:
        extra = 'forbid'

class FieldOverride(BaseModel):
    generator: str
    params: GeneratorParams

    # pylint: disable = no-self-argument
    @field_validator('generator')
    def validate_generator_name(cls, v):
        valid_generators = [
            'generate_text',
            'generate_keyword',
            'generate_integer',
            'generate_long',
            'generate_short',
            'generate_byte',
            'generate_float',
            'generate_double',
            'generate_boolean',
            'generate_date',
            'generate_ip',
            'generate_geo_point',
            'generate_object',
            'generate_nested',
            'generate_knn_vector',
            'generate_sparse_vector'
        ]

        if v not in valid_generators:
            raise ValueError(f"Generator '{v}' mentioned in FieldOverrides not among valid generators: {valid_generators}")
        return v

class MappingGenerationValuesConfig(BaseModel):
    generator_overrides: Optional[Dict[str, GeneratorParams]] = None
    field_overrides: Optional[Dict[str, FieldOverride]] = None

    # pylint: disable = no-self-argument
    @field_validator('generator_overrides')
    def validate_generator_types(cls, v):
        # Based on this documentation from OpenSearch: https://docs.opensearch.org/latest/mappings/supported-field-types/index/
        # TODO: Add more support for
        if v is not None:
            supported_mapping_field_types = {
                'core-field-types': ['boolean'],
                'string-based-field-types': ['text', 'keyword'],
                'numeric-field-types': ['byte', 'short', 'integer', 'long', 'float', 'double'],
                'date-time-field-types': ['date'],
                'ip-field-types': ['ip'],
                'geographic-field-types': ['geo_point'],
                'object-field-types': ['object', 'nested'],
                'vector-field-types': ['knn_vector', 'sparse_vector']
            }
            valid_generator_types = []

            for field_types in supported_mapping_field_types.values():
                valid_generator_types.extend(field_types)

            for generator_type in v.keys():
                if generator_type not in valid_generator_types:
                    raise ValueError(f"Invalid Generator Type '{generator_type}. Must be one of: {valid_generator_types}'")

        return v

    # pylint: disable = no-self-argument
    @field_validator('field_overrides')
    def validate_field_names(cls, v):
        if v is not None:
            for field_name in v.keys():
                if not re.match(r'^[a-zA-Z][a-zA-Z0-9_.]*$', field_name):
                    raise ValueError(f"Invalid Field Name '{field_name}' in FieldOverrides. Only alphanumeric characters, underscores and periods are allowed.")

        return v

class SyntheticDataGeneratorMetadata(BaseModel):
    index_name: Optional[str] = None
    index_mappings_path: Optional[str] = None
    custom_module_path: Optional[str] = None
    custom_config_path: Optional[str] = None
    output_path: Optional[str] = None
    total_size_gb: Optional[int] = None

    class Config:
        extra = 'forbid'

class SDGConfig(BaseModel):
    # If user does not provide YAML fil or provides YAML without all settings fields, it will use default generation settings.
    settings: Optional[SettingsConfig] = Field(default_factory=SettingsConfig)
    CustomGenerationValues: Optional[CustomGenerationValuesConfig] = None
    MappingGenerationValues: Optional[MappingGenerationValuesConfig] = None

    class Config:
        extra = 'forbid'
