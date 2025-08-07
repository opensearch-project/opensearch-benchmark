
import os
import multiprocessing

class IngestionManager:
    plimsoll = 4 * os.cpu_count()
    ballast = plimsoll/2
    lock = multiprocessing.Lock()
    rd_index = multiprocessing.Value('i', 0)
    wr_count = multiprocessing.Value('i', 0)
    producer_started = multiprocessing.Value('i', 0)
    load_full = multiprocessing.Condition()
    load_empty = multiprocessing.Condition()
