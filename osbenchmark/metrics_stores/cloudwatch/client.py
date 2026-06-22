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
boto3 client construction and credential resolution for the CloudWatch
metrics store. Builds CloudWatch Logs and CloudWatch Metrics clients via
boto3's default credential provider chain (env vars / profile / role / IMDS),
honoring the optional `datastore.profile` and `datastore.role_arn` config
keys.

Also exposes the `sts:GetCallerIdentity` startup probe so OSB can log which
AWS account and identity it is writing to before any data ships.
"""
