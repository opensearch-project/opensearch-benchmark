# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
# Licensed to Elasticsearch B.V. under one or more contributor
# license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright
# ownership. Elasticsearch B.V. licenses this file to you under
# the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#	http://www.apache.org/licenses/LICENSE-2.0

from typing import Optional

from osbenchmark.cloud_provider.cloud_provider import CloudProvider
from osbenchmark.cloud_provider.vendors import aws

class CloudProviderFactory:

    providers = [
        aws.AWSProvider()
    ]

    @classmethod
    def get_provider_from_client_options(cls, client_options) -> Optional[CloudProvider]:
        for provider in cls.providers:
            if provider.validate_client_options(client_options):
                return provider

        return None

    @classmethod
    def get_provider_from_config(cls, config) -> Optional[CloudProvider]:
        for provider in cls.providers:
            if provider.validate_config_for_metrics(config):
                return provider

        return None
