from __future__ import division
import datetime
import numpy as np
from math import ceil, floor


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
        :param rates: if given a list of floats, it means rates are given in tweets per hour and
                      time slots are going to be 1 hour each,
                      can be also a sub-intensity
        """
        if rates is not None:
            if type(rates[0]) is not dict:
                self.intensity = [{"rate": rate, "length": 1.} for rate in rates]
            else:
                self.intensity = rates
        else:
            self.intensity = []

    def append(self, rate, length):
        self.intensity.append({"rate": rate, "length": length})
        return self

    def size(self):
        return len(self.intensity)

    def total_time(self):
        return sum([item['length'] for item in self.intensity])

    def total_rate(self):
        return sum([item['rate'] for item in self.intensity])

    def copy_lengths(self, other):
        for i in range(self.size()):
            self[i]['length'] = other[i]['length']
        return self

    def get_as_vector(self):
        """
        :return tuple of rates and lengths arrays in numpy format
        """
        return (np.array([item['rate'] for item in self.intensity]),
                np.array([item['length'] for item in self.intensity]))
    
    def sub_intensity(self, start, end):
        """
        :param start: defines the starting hour
        :param end: defines the ending our
        :return: all intervals between start and end
        """
        t, i = 0.0, 0
        while t < start:
            t += self[i]['length']
            i += 1
        j = i
        while t < end:
            t += self[j]['length']
            j += 1
        return Intensity(self[i:j])

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
        if type(times) is list:
            self.tweet_times += times
        elif isinstance(times, TweetList):
            self.tweet_times += times.tweet_times
        else:
            raise TypeError('Unknown type %s: Cannot append to tweet list' % type(times))
        
    def sort(self):
        self.tweet_times.sort()
        
    def sublist(self, start_time, end_time):
        start = long(start_time.strftime('%s'))
        end = long(end_time.strftime('%s'))
        lst = []
        for t in self.tweet_times:
            if end >= t >= start:
                lst.append(t)
#         lst.sort()
        return lst

    def get_periodic_intensity(self, period_length=24 * 7, time_slots=0, start_time=None, end_time=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :param start_time: The start of time that we want to learn the intensity
        :param end_time: The end of time that we want to learn the intensity
        :return: intensity in the period length
        """

        if start_time is None:
            start_time = datetime.datetime.fromtimestamp(0)
        if end_time is None:
            end_time = datetime.datetime.fromtimestamp(max(self.tweet_times))
        if time_slots is None:
            time_slots = [1.] * period_length

        assert sum(time_slots) == period_length

        total_time = (self[-1] - self[0]) / 3600.
        total_number_of_periods = ceil(total_time / period_length)

        tweets_per_slot = [0] * len(time_slots)
        time_window = self.sublist(start_time, end_time)

        for time in time_window:
            tweets_per_slot[find_interval(time, period_length, time_slots)] += 1

        intensity = Intensity()

        for i in range(len(time_slots)):
            intensity.append(rate=tweets_per_slot[i] / total_number_of_periods / time_slots[i],
                             length=time_slots[i])

        return intensity

    def get_connection_probability(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: probability of being online in each time slot during period length
        """

        if time_slots is None:
            time_slots = [1.] * period_length

        assert sum(time_slots) == period_length

        bags = [None] * len(time_slots)
        for i in range(len(time_slots)):
            bags[i] = set()

        max_period_id, min_period_id = 0, 100000

        for time in self.tweet_times:
            period_id = int(time / period_length / 3600)
            bags[find_interval(time, period_length, time_slots)].add(period_id)

            max_period_id = period_id if max_period_id < period_id else max_period_id
            min_period_id = period_id if min_period_id > period_id else min_period_id

        period_count = max_period_id - min_period_id + 1

        return [len(bag) / period_count for bag in bags]


def find_interval(tweet_time, period_length, time_slots):
    # With assumption of equal time intervals...
    return int(floor((tweet_time % (period_length * 3600)) / (3600. * time_slots[0])))

#     time_in_period = tweet_time % (period_length * 3600)
#     t, i = 0, 0
#     while t < time_in_period:
#         t += time_slots[i] * 3600.
#         i += 1
#     return i - 1


def main():
    times = np.array([0.8, 0.9, 3.1, 5.2, 7.8, 8.3, 11.4, 13.2, 15.6, 19.2, 21.3]) * 3600.
    list = TweetList(times)
    print list.get_connection_probability(8)

if __name__ == '__main__':
    main()
