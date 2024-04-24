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
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

ERROR_RATE_KEY = "error rate"
VALUE_KEY = "Value"


class Result:
    def __init__(self, test_id, batch_size, bulk_size, number_of_client):
        self.success = None
        self.test_id = test_id
        self.batch_size = batch_size
        self.bulk_size = bulk_size
        self.number_of_client = number_of_client
        self.total_time = 0
        self.error_rate = 0
        self.output = None

    def set_output(self, success, total_time, output):
        self.success = success
        self.total_time = total_time
        if not output:
            return
        self.output = output
        if output and ERROR_RATE_KEY in output:
            self.error_rate = float(output[ERROR_RATE_KEY][VALUE_KEY])

    def __str__(self):
        return f"bulk size: {self.bulk_size}, batch size: {self.batch_size}, number of clients: {self.number_of_client}"
