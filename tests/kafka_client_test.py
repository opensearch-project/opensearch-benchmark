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

from unittest import TestCase, mock

from osbenchmark.kafka_client import KafkaMessageProducer
from osbenchmark.client import MessageProducerFactory
from tests import run_async


class KafkaMessageProducerTests(TestCase):
    @run_async
    @mock.patch("aiokafka.AIOKafkaProducer")
    async def test_create_producer_with_valid_params(self, mock_aio_kafka_producer_class):
        mock_producer_instance = mock.AsyncMock()
        mock_aio_kafka_producer_class.return_value = mock_producer_instance

        params = {
            "ingestion-source": {
                "type": "kafka",
                "param": {
                    "topic": "test-topic",
                    "bootstrap-servers": "localhost:9092"
                }
            }
        }

        producer = await KafkaMessageProducer.create(params)

        mock_aio_kafka_producer_class.assert_called_once_with(
            bootstrap_servers="localhost:9092",
            key_serializer=str.encode,
            value_serializer=str.encode
        )
        mock_producer_instance.start.assert_awaited_once()
        self.assertIsInstance(producer, KafkaMessageProducer)
        self.assertEqual("test-topic", producer._topic)

    @run_async
    async def test_create_producer_missing_topic_raises_error(self):
        params = {
            "ingestion-source": {
                "type": "kafka",
                "param": {
                    # no "topic" entry
                    "bootstrap-servers": "localhost:9092"
                }
            }
        }

        with self.assertRaisesRegex(ValueError, "No 'topic' specified"):
            await KafkaMessageProducer.create(params)

    @run_async
    async def test_send_message(self):
        mock_producer = mock.AsyncMock()
        topic = "test-topic"
        producer = KafkaMessageProducer(mock_producer, topic)

        await producer.send_message("test", key="key")
        mock_producer.send_and_wait.assert_awaited_once_with(
            topic, "test", key="key"
        )

    @run_async
    async def test_stop(self):
        mock_producer = mock.AsyncMock()
        topic = "test-topic"
        producer = KafkaMessageProducer(mock_producer, topic)
        await producer.stop()
        mock_producer.stop.assert_awaited_once()


class MessageProducerFactoryTests(TestCase):
    @run_async
    @mock.patch("aiokafka.AIOKafkaProducer")
    async def test_create_kafka_producer_via_factory(self, mock_aio_kafka_producer_class):
        mock_producer_instance = mock.AsyncMock()
        mock_aio_kafka_producer_class.return_value = mock_producer_instance
        params = {
            "ingestion-source": {
                "type": "kafka",
                "param": {
                    "topic": "factory-topic",
                    "bootstrap-servers": "localhost:9092"
                }
            }
        }

        producer = await MessageProducerFactory.create(params)
        # The returned instance should be a KafkaMessageProducer
        self.assertIsInstance(producer, KafkaMessageProducer)
        self.assertEqual("factory-topic", producer._topic)
        mock_aio_kafka_producer_class.assert_called_once()

    @run_async
    async def test_create_unsupported_type_raises_error(self):
        params = {
            "ingestion-source": {
                "type": "unknown",
                "param": {
                    "topic": "test"
                }
            }
        }
        with self.assertRaisesRegex(ValueError, "Unsupported ingestion source type: unknown"):
            await MessageProducerFactory.create(params)
