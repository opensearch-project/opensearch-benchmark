# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
import yaml
from pydantic import BaseModel, Field, validator, ValidationError
# from dataclasses import dataclass, field
from typing import Optional, Dict, List, Any, Union
from datetime import datetime
import re

GB_TO_BYTES = 1024 ** 3

# DEFAULT_GENERATION_SETTINGS = {
#     "workers": os.cpu_count(),
#     "max_file_size_gb": 40,
#     "docs_per_chunk": 10000
# }

class SettingsConfig(BaseModel):
    workers: Optional[int] = Field(default_factory=os.cpu_count)
    max_file_size_gb = Optional[int] = 40
    docs_per_chunk: Optional[int] = 10000

    @validator('workers', 'max_file_size_gb', 'docs_per_chunk')
    def validate_values_are_positive_integers(cls, v):
        if v is not None and v <= 0:
            raise ValueError('Value in Settings portion must be a positive integer.')

        return v

class CustomGenerationValuesConfig(BaseModel):
    custom_lists: Optional[Dict[str, List[Any]]] = None
    custom_providers: Optional[List[Any]] = None

    @validator('custom_lists')
    def validate_custom_lists(cls, v):
        if v is not None:
            for key, value in v.items():
                if not isinstance(key, str):
                    raise ValueError('All keys within custom_lists of CustomGenerationValues section must be strings')
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

    class Config:
        extra = 'forbid'


class FieldOverride(BaseModel):
    generator: str
    params: GeneratorParams

    @validator('generator')
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
            'genearte_date',
            'generate_ip',
            'generate_geopoint',
            'generate_object',
            'generate_nested'
        ]

        if v not in valid_generators:
            raise ValueError(f"Generator '{v}' mentioned in FieldOverrides not among valid generators: {valid_generators}")
        return v

class MappingGenerationValuesConfig(BaseModel):
    generator_overrides: Optional[Dict[str, GeneratorParams]] = None
    field_overrides: Optional[Dict[str, FieldOverride]] = None

    @validator('generator_overrides')
    def validate_generator_types(cls, v):
        if v is not None:
            valid_generator_types = ['integer', 'long', 'float', 'double', 'date', 'text', 'keyword']

            for generator_type in v.keys():
                if generator_type not in valid_generator_types:
                    raise ValueError(f"Invalid Generator Type '{generator_type}. Must be one of: {valid_generator_types}'")

        return v

    @validator('field_overrides')
    def validate_field_names(cls, v):
        if v is not None:
            for field_name in v.keys():
                if not re.match(r'^[a-zA-Z][a-zA-Z0-9_.]*$', field_name):
                    raise ValueError(f"Invalid Field Name in FieldOverrides: {field_name}")

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
    settings: Optional[SettingsConfig] = None
    CustomGenerationValues: Optional[CustomGenerationValuesConfig] = None
    MappingGenerationValues: Optional[MappingGenerationValuesConfig] = None
