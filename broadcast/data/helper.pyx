from __future__ import division

import numpy as np
cimport numpy as np


def get_intensity_cy(np.ndarray times, int period_length):
    cdef int i = 0
    cdef np.ndarray tweets_per_slot = np.zeros(period_length, dtype=np.long)

    for i in range(times.shape[0]):
        interval = find_interval(times[i], period_length)
        tweets_per_slot[interval] += 1

    return tweets_per_slot


cdef int find_interval(int tweet_time, int period_length):
    return int(tweet_time / 3600) % period_length


def get_connection_bags_cy(np.ndarray times, int period_length):
    cdef int i = 0
    cdef long prev_time = 0
    
    cdef np.ndarray bags = np.zeros(period_length, dtype=np.int)
    for i in range(times.shape[0]):
        if times[i] - prev_time < 3600 and times[i] % 3600 > prev_time % 3600:
            continue

        interval = find_interval(times[i], period_length)
        bags[interval] += 1

        prev_time = times[i]
    return bags
