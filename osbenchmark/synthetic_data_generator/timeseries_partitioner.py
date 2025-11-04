# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
import logging
import math
from collections import deque
import sys
from typing import Generator
import time

import pandas as pd

import osbenchmark.exceptions as exceptions

class TimeSeriesPartitioner:

    # TODO: Change this into a dictionary that points to which frequencies can have which formats
    VALID_DATETIMESTAMPS_FORMATS = [
        "%Y-%m-%d",                 # 2023-05-20
        "%Y-%m-%dT%H:%M:%S",        # 2023-05-20T15:30:45
        "%Y-%m-%dT%H:%M:%S.%f",     # 2023-05-20T15:30:45.123456
        "%Y-%m-%d %H:%M:%S",        # 2023-05-20 15:30:45
        "%Y-%m-%d %H:%M:%S.%f",     # 2023-05-20 15:30:45.123456
        "%d/%m/%Y",                 # 20/05/2023
        "%m/%d/%Y",                 # 05/20/2023
        "%d-%m-%Y",                 # 20-05-2023
        "%m-%d-%Y",                 # 05-20-2023
        "%d.%m.%Y",                 # 20.05.2023
        "%Y%m%d",                   # 20230520
        "%B %d, %Y",                # May 20, 2023
        "%b %d, %Y",                # May 20, 2023
        "%d %B %Y",                 # 20 May 2023
        "%d %b %Y",                 # 20 May 2023
        "%Y %B %d",                 # 2023 May 20
        "%d/%m/%Y %H:%M",           # 20/05/2023 15:30
        "%d/%m/%Y %H:%M:%S",        # 20/05/2023 15:30:45
        "%Y-%m-%d %I:%M %p",        # 2023-05-20 03:30 PM
        "%d.%m.%Y %H:%M",           # 20.05.2023 15:30
        "%H:%M",                    # 15:30
        "%H:%M:%S",                 # 15:30:45
        "%I:%M %p",                 # 03:30 PM
        "%I:%M:%S %p",              # 03:30:45 PM
        "%a, %d %b %Y %H:%M:%S",    # Sat, 20 May 2023 15:30:45
        "%Y/%m/%d",                 # 2023/05/20
        "%Y/%m/%d %H:%M:%S",        # 2023/05/20 15:30:45
        "%Y%m%d%H%M%S",             # 20230520153045
        "epoch_s",                  # Epoch time in seconds format
        "epoch_ms"                  # Epoch time in ms format
    ]

    # TODO: Let's make this a hashmap so that we can ensure the invalid formats are not used (e.g. frequency is updated to ms and format is still seconds)
    # These frequencies are based on what is supported in the Pandas library
    AVAILABLE_FREQUENCIES = ['B', 'C', 'D', 'h', 'bh', 'cbh', 'min', 's', 'ms']

    def __init__(self, timeseries_enabled: dict, workers: int, docs_per_chunk: int, avg_document_size: int, total_size_bytes: int):
        self.timeseries_enabled = timeseries_enabled
        self.workers = workers
        self.docs_per_chunk = docs_per_chunk
        self.avg_document_size = avg_document_size
        self.total_size_bytes = total_size_bytes

        self.timeseries_field = self.timeseries_enabled.timeseries_field
        self.start_date = self.timeseries_enabled.timeseries_start_date if self.timeseries_enabled.timeseries_start_date else "1/01/2019"
        self.end_date = self.timeseries_enabled.timeseries_end_date if self.timeseries_enabled.timeseries_end_date else "12/31/2019"
        self.frequency = self.timeseries_enabled.timeseries_frequency if self.timeseries_enabled.timeseries_frequency else "min"
        self.format = self.timeseries_enabled.timeseries_format if self.timeseries_enabled.timeseries_format else "%Y-%m-%dT%H:%M:%S"
        self.logger = logging.getLogger(__name__)

        if self.frequency not in TimeSeriesPartitioner.AVAILABLE_FREQUENCIES:
            msg = f"Frequency {self.frequency} not found in available frequencies {TimeSeriesPartitioner.AVAILABLE_FREQUENCIES}"
            raise exceptions.ConfigError(msg)

        if self.format not in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
            msg = f"Format {self.format} not found in available format {TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS}"
            raise exceptions.ConfigError(msg)

    def get_updated_settings(self, timeseries_settings) -> dict:
        timeseries_settings.model_update(timeseries_field=self.timeseries_field)
        timeseries_settings.model_update(timeseries_start_date=self.start_date)
        timeseries_settings.model_update(timeseries_end_date=self.end_date)
        timeseries_settings.model_update(timeseries_frequency=self.frequency)
        timeseries_settings.model_update(timeseries_format=self.format)

        return timeseries_settings

    def create_window_generator(self) -> Generator:
        '''
        returns: a list of timestamp pairs where each timestamp pair is a set containing start datetime and end datetime
        '''
        # Determine optimal time settings
        # Check if number of docs generated will fit in the timestamp. Adjust frequency as needed
        expected_number_of_docs = self.total_size_bytes // self.avg_document_size
        expected_number_of_docs_with_buffer = math.ceil((expected_number_of_docs * 0.1) + expected_number_of_docs)

        # Get number of timestamps with dates and frequencies
        number_of_timestamps = self._count_timestamps(frequency=self.frequency)

        if number_of_timestamps < expected_number_of_docs_with_buffer:
            self.logger.info("Number of timestamps generated is less than expected docs generated. Trying to find the optimal frequency")
            # ms is the smallest unit of time SDG can generate
            if self.frequency == 'ms':
                msg = "No finer time frequencies available to try than \"ms\". Please expand dates and frequency accordingly."
                self.logger.error(msg)
                raise exceptions.ConfigError(msg)

            #TODO: Update the timeseries enabled settings too so downstream isn't confused
            optimal_frequency = self._try_other_frequencies(expected_number_of_docs_with_buffer)
            if not self._does_user_want_optimal_frequency(user_frequency=self.frequency, optimal_frequency=optimal_frequency):
                self.logger.info("User does not want to use optimal frequency and will cancel generation.")
                sys.exit(1)

            self.frequency = optimal_frequency
            print("Frequency chosen: ", self.frequency)
            self.logger.info("Updated frequency to use [%s]", self.frequency)

        # After validating everything, let's return the window generator
        return self.generate_datetimestamp_window()

    def generate_datetimestamp_window(self):
        current = pd.Timestamp(self.start_date)
        end = pd.Timestamp(self.end_date)
        freq = pd.Timedelta(f"{self.docs_per_chunk-1}{self.frequency}") # Need to subtract one to include current timestamp.

        while current < end:
            window_end = min(current + freq, end)
            yield (current, window_end)
            current += freq

    @staticmethod
    def generate_datetimestamps_from_window(window: set, frequency: str = "min", format: str = "%Y-%m-%dT%H:%M:%S") -> Generator:
        if frequency not in TimeSeriesPartitioner.AVAILABLE_FREQUENCIES:
            msg = f"Frequency {frequency} not found in available frequencies {TimeSeriesPartitioner.AVAILABLE_FREQUENCIES}"
            raise exceptions.ConfigError(msg)

        if format not in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
            msg = f"Format {format} not found in available format {TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS}"
            raise exceptions.ConfigError(msg)

        try:
            start_datetimestamp = window[0]
            end_datetimestamp = window[1]
            generated_datetimestamps: pd.DatetimeIndex = pd.date_range(start_datetimestamp, end_datetimestamp, freq=frequency)
            #TODO: Handle formatting after generating iterator?
            if format and format in TimeSeriesPartitioner.VALID_DATETIMESTAMPS_FORMATS:
                if format == "epoch_s":
                    generated_datetimestamps = generated_datetimestamps.map(lambda x: int(x.timestamp()))
                elif format == "epoch_ms":
                    generated_datetimestamps = generated_datetimestamps.map(lambda x: int(x.timestamp() * 1000))
                else:
                    generated_datetimestamps = generated_datetimestamps.strftime(date_format=format)

            return generated_datetimestamps

        except IndexError:
            raise exceptions.SystemSetupError("IndexError encountered with accessing datetimestamp from window.")
        except Exception:
            raise exceptions.SystemSetupError("Unknown error encountered with generating datetimestamps from window.")

    @staticmethod
    def sort_results_by_datetimestamps(results: list, timeseries_field: str) -> list:
        logger = logging.getLogger(__name__)
        logger.info("Length of results: %s", len(results))
        logger.info("Docs in each result: %s ", [len(result) for result in results])


        start_time = time.time()
        sorted_results = sorted(results, key=lambda chunk: chunk[0][timeseries_field])
        end_time = time.time()
        logger.info("Time it took to sort: %s secs", end_time-start_time)
        logger.info("First timestamp from all chunks: %s ", [result[0][timeseries_field] for result in sorted_results])

        return sorted_results

    def _count_timestamps(self, frequency: str) -> int:
        if frequency in ["B", "C", "bh", "cbh"]:
            try:
                return len(pd.date_range(start=self.start_date, end=self.end_date, freq=frequency))
            except Exception as e:
                msg = f"Had issues when generating and counting datetimestamps: {e}"
                raise exceptions.SystemSetupError(msg)

        else:
            # Arithmetically calculate rather than load into memory
            start_datetimestamp = pd.Timestamp(self.start_date)
            end_datetimestamp = pd.Timestamp(self.end_date)

            offset = pd.tseries.frequencies.to_offset(freq=frequency)
            delta = end_datetimestamp - start_datetimestamp
            count = int(delta / offset) + 1
            return count


    def _try_other_frequencies(self, expected_number_of_docs_with_buffer: int) -> str:
        frequencies_to_try = deque(TimeSeriesPartitioner.AVAILABLE_FREQUENCIES[TimeSeriesPartitioner.AVAILABLE_FREQUENCIES.index(self.frequency)+1:])

        frequency = ""
        while frequencies_to_try:
            frequency = frequencies_to_try.popleft()
            number_of_timestamps = self._count_timestamps(frequency=frequency)
            print("Number of docs expected, frequency, and number of timestamps: ", expected_number_of_docs_with_buffer, frequency, number_of_timestamps)
            if number_of_timestamps > expected_number_of_docs_with_buffer:
                self.logger.info("Using [%s] frequency as this resulted in more timestamps", frequency)
            else:
                self.logger.info("Using [%s] frequency did not result in more timestamps", frequency)

        return frequency

    def _does_user_want_optimal_frequency(self, user_frequency: str, optimal_frequency: str) -> bool:
        valid_responses = ['y', 'yes', 'n', 'no']
        msg = f"The frequency [{optimal_frequency}] is a better option for the number of docs you are trying to generate " + \
            "because the current frequency you've selected does not have enough timestamps to allocate to docs generated." + \
            f"If you prefer your current frequency [{user_frequency}], please extend the time frame. " + \
            f"Would you like to use [{optimal_frequency}] as the frequency? (y/n): "
        requested_input = input(msg)
        while requested_input.lower() not in valid_responses:
            msg = f"Please enter y or n. The frequency [{optimal_frequency}] is a better option for the number of docs you are trying to generate. " + \
            f"If you prefer your current frequency [{user_frequency}], please extend the time frame. " + \
            f"Would you like to use [{optimal_frequency}] as the frequency? (y/n): "
            requested_input = input(msg)

        return requested_input.lower() in ['y', 'yes']
