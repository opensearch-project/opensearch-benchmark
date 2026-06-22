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
Disk spool for the CloudWatch metrics store.

When CloudWatch shipping fails persistently mid-run due to credential
errors (e.g. an expired SSO session or a revoked role), the store switches
into spool mode: pre-formatted EMF events are appended to newline-delimited
JSON files under the configured spool directory (default
``~/.osb/cw-spool/<test-run-id>/``) instead of being shipped to CloudWatch
Logs. A background thread probes sts:GetCallerIdentity to auto-recover and
drain the spool while the run is still in progress.

If the run ends while still spooled, the local data is preserved for later
replay via `opensearch-benchmark cw-replay`.
"""
