from osbenchmark import time


class PeriodicWaiter:
    def __init__(self, poll_interval, poll_timeout, clock=time.Clock):
        self.poll_interval = poll_interval
        self.poll_timeout = poll_timeout
        self.clock = clock

    def wait(self, poll_function, *poll_function_args, **poll_function_kwargs):
        stop_watch = self.clock.stop_watch()
        stop_watch.start()

        while stop_watch.split_time() < self.poll_timeout:
            if poll_function(*poll_function_args, **poll_function_kwargs):
                return
            time.sleep(self.poll_interval)

        raise TimeoutError
