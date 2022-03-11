import sys
from unittest import TestCase
from unittest.mock import Mock

from osbenchmark.utils.periodic_waiter import PeriodicWaiter


class PeriodicWaiterTest(TestCase):
    def setUp(self):
        self.polling_function = Mock()

        stop_watch = IterationBasedStopWatch(max_iterations=2)
        clock = TestClock(stop_watch=stop_watch)

        self.periodic_waiter = PeriodicWaiter(0, 2, clock=clock)

    def test_success_before_timeout(self):
        self.polling_function.side_effect = [False, True]

        self.periodic_waiter.wait(self.polling_function)

    def test_timeout(self):
        self.polling_function.side_effect = [False, False]

        with self.assertRaises(TimeoutError):
            self.periodic_waiter.wait(self.polling_function)


class IterationBasedStopWatch:
    __test__ = False

    def __init__(self, max_iterations):
        self.iterations = 0
        self.max_iterations = max_iterations

    def start(self):
        self.iterations = 0

    def split_time(self):
        if self.iterations < self.max_iterations:
            self.iterations += 1
            return 0
        else:
            return sys.maxsize


class TestClock:
    __test__ = False

    def __init__(self, stop_watch):
        self._stop_watch = stop_watch

    def stop_watch(self):
        return self._stop_watch
