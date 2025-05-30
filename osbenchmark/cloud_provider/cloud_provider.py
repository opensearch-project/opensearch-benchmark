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

from abc import ABC, abstractmethod

class CloudProvider(ABC):

    @abstractmethod
    def validate_client_options(self, client_options: dict) -> bool:
        pass

    @abstractmethod
    def validate_config_for_metrics(self, config) -> bool:
        pass

    @abstractmethod
    def mask_client_options(self, masked_client_options: dict, client_options: dict) -> dict:
        pass

    @abstractmethod
    def parse_log_in_params(self, client_options=None, config=None, for_metrics_datastore=False) -> dict:
        pass

    @abstractmethod
    def update_client_options_for_metrics(self, client_options) -> dict:
        pass

    @abstractmethod
    def create_client(self, hosts, client_options, client_class=None, use_async=False):
        pass
