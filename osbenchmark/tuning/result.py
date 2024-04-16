ERROR_RATE_KEY = "error rate"


class Result(object):
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
        self.error_rate = float(output[ERROR_RATE_KEY]) if ERROR_RATE_KEY in output else 0 # percentage

