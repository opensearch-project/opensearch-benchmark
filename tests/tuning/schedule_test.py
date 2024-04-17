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

from unittest import TestCase
from osbenchmark.tuning.schedule import Schedule, ScheduleRunner


class TestSchedule(TestCase):
    def test_Schedule_with_batch_size(self):
        schedule = Schedule("1", None, 0, 0, 0)
        self.assertEqual([1], schedule.steps)

    def test_Schedule_with_schedule_val(self):
        schedule = Schedule(None, "10:100:1:10", 0, 0, 0)
        self.assertEqual(list(range(10, 101, 10)), schedule.steps)

        schedule = Schedule("1", "10:100:-11:10", 0, 0, 0)
        self.assertEqual(list(range(100, 9, -10)), schedule.steps)

        schedule = Schedule("1", "@10:20:100", 0, 0, 0)
        self.assertEqual([10, 20, 100], schedule.steps)

        schedule = Schedule(None, "10", 0, 100, 20)
        self.assertEqual([10, 30, 50, 70, 90, 100], schedule.steps)


class FakeSchedule:
    def __init__(self, steps):
        self.steps = steps


def fake_callback(args, test_id, arg1, arg2):
    return {"args": args, "arg1": arg1, "arg2": arg2}


class TestScheduleRunner(TestCase):
    def test_ScheduleRunner(self):
        schedule1 = FakeSchedule([1, 2])
        schedule2 = FakeSchedule([4, 5])
        args = {}
        runner = ScheduleRunner(args, schedule1, schedule2)
        results = runner.run(fake_callback).values()
        self.assertEqual({(result["arg1"], result["arg2"]) for result in results}, {(1,4), (2,4), (1,5), (2,5)})
