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
CloudWatchMetricsStore — concrete MetricsStore that ships every sample to
CloudWatch Logs as an EMF event.

Buffers samples in memory and flushes to CloudWatch Logs via PutLogEvents
when the batch reaches CloudWatch's per-call limits (10,000 events or 1 MiB
of payload, accounting for the per-event overhead) or when an explicit
flush is requested. Read methods (get_one, get_stats, get_percentiles,
get_error_rate) execute CloudWatch Logs Insights queries against the
configured metrics log group.
"""
