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

"""
Embedded Metric Format (EMF) document builder.

Transforms OSB metric documents (shape: `{name, value, ...}`) into EMF log
events with an `_aws.CloudWatchMetrics` block so CloudWatch Logs
auto-extracts the numeric value as a CloudWatch metric.

Also handles the multi-directive grouping needed for telemetry payloads
(NodeStats etc.) which can exceed EMF's 100-metric-per-directive limit.

Pure transform — no I/O, no boto3 dependency. Unit-testable in isolation.
"""
