from __future__ import division
import numpy as np
from math import ceil


class Intensity:
    """
    The intensity unit is tweets per hour and time slots are also expressed in hour.
    Intensity is of the form:
        [{
            "rate": intensity value in tweets per hour,
            "length": time slot length in hours
        },
        ...]
    """

    def __init__(self, rates=None):
        """
        :param rates: if given a list, it means rates are given in tweets per hour and
                      time slots are going to be 1 hour each
        """
        if rates:
            self.intensity = [{"rate": rate, "length": 1.} for rate in rates]
        else:
            self.intensity = []

    def append(self, rate, length):
        self.intensity.append({"rate": rate, "length": length})

    def size(self):
        return len(self.intensity)

    def total_time(self):
        return sum([item['length'] for item in self.intensity])

    def get_as_vector(self):
        """
        :return tuple of rates and lengths arrays in numpy format
        """
        return (np.array([item['rate'] for item in self.intensity]),
                np.array([item['length'] for item in self.intensity]))

    def __getitem__(self, item):
        return self.intensity[item]

    def __str__(self):
        return ', '.join(['(%.3f, %.2f)' % (item['rate'], item['length']) for item in self.intensity])


class TweetList:
    def __init__(self, times=None):
        """
        Time is in unix timestamp format, and so is in seconds precision
        :param times: time list, can be none
        """
        self.tweet_times = [] if times is None else times

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)

    def __getitem__(self, item):
        return self.tweet_times[item]

    def append_to(self, times):
        self.tweet_times += times

    def get_periodic_intensity(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: intensity in the period length
        """

        if time_slots is None:
            time_slots = [1.] * period_length

        assert sum(time_slots) == period_length

        total_time = (self[-1] - self[0]) / 3600.
        total_number_of_periods = ceil(total_time / period_length)

        tweets_per_slot = [0] * len(time_slots)

        for time in self.tweet_times:
            tweets_per_slot[find_interval(time, period_length, time_slots)] += 1

        intensity = Intensity()

        for i in range(len(time_slots)):
            intensity.append(rate=tweets_per_slot[i] / total_number_of_periods / time_slots[i],
                             length=time_slots[i])

        return intensity


def find_interval(tweet_time, period_length, time_slots):
    time_in_period = tweet_time % (period_length * 3600)
    t, i = 0, 0
    while t < time_in_period:
        t += time_slots[i] * 3600.
        i += 1
    return i - 1


def main():
    pass

if __name__ == '__main__':
    main()
