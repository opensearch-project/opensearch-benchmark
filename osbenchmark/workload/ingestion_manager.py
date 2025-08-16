# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.

import os
import multiprocessing

class IngestionManager:
    plimsoll = 4 * os.cpu_count()
    ballast = plimsoll/2
    chunk_size = 50                     # in MB
    lock = multiprocessing.Lock()
    rd_index = multiprocessing.Value('i', 0)
    wr_count = multiprocessing.Value('i', 0)
    producer_started = multiprocessing.Value('i', 0)
    load_full = multiprocessing.Condition()
    load_empty = multiprocessing.Condition()
