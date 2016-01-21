from __future__ import division
import bisect
import datetime
from math import ceil, floor
from pprint import pprint
import random
import numpy as np
from util.cal import unix_timestamp
from util.decorators import cache_enabled


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


class ITweetList(object):
    _current = 0  # used in iterator

    def __getitem__(self, item):
        raise NotImplementedError()

    def __len__(self):
        raise NotImplementedError()

    def __iter__(self):
        self._current = 0
        return self

    def next(self):
        if self._current >= len(self):
            raise StopIteration
        else:
            self._current += 1
            return self[self._current - 1]

    def sublist(self, start_date=None, end_date=None):
        """
        :param start_date: The start of time that we want to slice (should be start of a day)
        :param end_date: The end of time that we want to slice (should be start of a day)
        :return: a TweetListView with tweets in range [start_date, end_date] "excluding" the end_date itself
        """

        start_unix = unix_timestamp(start_date, default_ts=0)
        if len(self) is 0:
            end_unix = unix_timestamp(end_date, default_ts=1254355200)
        else:
            end_unix = unix_timestamp(end_date, default_ts=max([0] + self[-1]))

        starting_tweet_index = self.get_slice_index_left(start_unix)
        ending_tweet_index = self.get_slice_index_right(end_unix)

        return TweetListView(self,
                             offset=starting_tweet_index,
                             size=ending_tweet_index - starting_tweet_index)

    def get_slice_index_left(self, time_ts):
        """ Return LOCAL index of slicing to the left """
        raise NotImplementedError()

    def get_slice_index_right(self, time_ts):
        """ Return LOCAL index of slicing to the right """
        raise NotImplementedError()

    def get_day_tweets(self, date):
        start_ts = int(unix_timestamp(date) / 86400) * 86400
        end_ts = start_ts + 86400

        slice_left = self.get_slice_index_left(start_ts)
        slice_right = self.get_slice_index_right(end_ts)

        return TweetListView(self, slice_left, slice_right - slice_left)

    def get_periodic_intensity(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: intensity in the period length (returnes the cached version if possible)
        """

        if time_slots is None:
            time_slots = [1.] * period_length

        tweets_per_slot = [0] * len(time_slots)

        if len(self) is 0:
            intensity = Intensity()
            for i in range(len(time_slots)):
                intensity.append(rate=0., length=time_slots[i])
            return intensity

        total_time = (self[-1] - self[0]) / 3600.
        total_number_of_periods = max(ceil(total_time / period_length), 1)

        for time in self:
            interval = ITweetList.find_interval(time, period_length, time_slots)
            tweets_per_slot[interval] += 1

        intensity = Intensity()

        for i in range(len(time_slots)):
            intensity.append(rate=tweets_per_slot[i] / total_number_of_periods / time_slots[i],
                             length=time_slots[i])
        return intensity

    @cache_enabled
    def get_connection_probability(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: probability of being online in each time slot during period length (cache support)
        """

        if time_slots is None:
            time_slots = [1.] * period_length

        if len(self) is 0:
            return [0.] * len(time_slots)

        bags = [0] * len(time_slots)

        prev_time = 0
        for time in self:
            if time - prev_time < 3600 and time % 3600 > prev_time % 3600:
                continue

            interval = ITweetList.find_interval(time, period_length, time_slots)
            bags[interval] += 1

            prev_time = time

        period_count = max(ceil((self[-1] - self[0]) / (period_length * 3600)), 1)
        return [bag / period_count for bag in bags]

    @staticmethod
    def find_interval(tweet_time, period_length, time_slots):
        # With assumption of equal time intervals...
        return int(floor((tweet_time % (period_length * 3600)) / (3600. * time_slots[0])))


class TweetList(ITweetList):
    def __init__(self, times=None, build_index=True, sort=True, index=None):
        """
        Time is in unix timestamp format, and so is in seconds precision
        :param times: time list, can be none, if ndarray remains the same
        """
        self._tweet_times = [] if times is None else np.array(times)

    def __len__(self):
        return len(self._tweet_times)

    def __getitem__(self, item):
        return self._tweet_times[item]
    
    def __str__(self):
        return 'TweetList (%d tweets) %s' % (len(self), str(self._tweet_times))

    def sort(self):
        self._tweet_times.sort()

    def get_slice_index_left(self, time_ts):
        return bisect.bisect_left(self._tweet_times, time_ts)

    def get_slice_index_right(self, time_ts):
        return bisect.bisect_right(self._tweet_times, time_ts)


class TweetListView(ITweetList):

    def __init__(self, tweet_list, offset=0, size=-1):
        """
        :type tweet_list: ITweetList
        """
        self.tweet_list = tweet_list
        self._offset = offset
        self._size = size if size >= 0 else (len(tweet_list) - offset)

    def __len__(self):
        return self._size

    def __getitem__(self, item):
        if item >= self._size or item < -self._size:
            raise IndexError('Index out of bounds for TweetListView')

        if item >= 0:
            return self.tweet_list[item + self._offset]
        else:
            return self.tweet_list[self._offset + self._size + item]

    def get_slice_index_right(self, time_ts):
        ind = self.tweet_list.get_slice_index_right(time_ts) - self._offset
        if ind < 0:
            return 0
        if ind >= self._size:
            return self._size
        return ind

    def get_slice_index_left(self, time_ts):
        ind = self.tweet_list.get_slice_index_left(time_ts) - self._offset
        if ind < 0:
            return 0
        if ind >= self._size:
            return self._size
        return ind
