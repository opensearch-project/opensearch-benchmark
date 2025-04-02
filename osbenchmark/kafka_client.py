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

from aiokafka import AIOKafkaProducer
from osbenchmark.context import RequestContextHolder

class KafkaMessageProducer:
    def __init__(self, producer, topic):
        self._producer = producer
        self._topic = topic
        self._ctx_holder = RequestContextHolder()

    @classmethod
    async def create(cls, params):
        """
        Creates a Kafka producer based on parameters in the ingestion source.
        """

        ingestion_source = params.get("ingestion-source", {})
        kafka_params = ingestion_source.get("param", {})
        topic = kafka_params.get("topic")
        if not topic:
            raise ValueError("No 'topic' specified in ingestion source parameters.")
        bootstrap_servers = kafka_params.get("bootstrap-servers", "")

        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=str.encode,
            value_serializer=str.encode
        )
        await producer.start()
        return cls(producer, topic)

    async def send_message(self, message, key=""):
        """
        Sends a message to the producer's topic.
        """
        await self._producer.send_and_wait(self._topic, message, key=key)

    async def stop(self):
        """
        Stops the underlying producer.
        """
        await self._producer.stop()

    @property
    def new_request_context(self):
        # Delegate to the internal holder
        return self._ctx_holder.new_request_context
