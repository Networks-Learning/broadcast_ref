from __future__ import division

import bisect
import datetime
from math import ceil, floor
from pprint import pprint
import random

import numpy as np

from util.cal import unix_timestamp


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
    def __init__(self, times=None, build_index=True, sort=True, index=None):
        """
        Time is in unix timestamp format, and so is in seconds precision
        :param times: time list, can be none, if ndarray remains the same
        """
        self.tweet_times = [] if times is None else np.array(times)
        self.index = {} if (build_index is False or index is None) else index
        self.index_keys = [] if self.index is {} else self.index.keys()

        self._intensity_cache_conf = None
        self._intensity_cache = None
        self._conn_prob_cache_conf = None
        self._conn_prob_cache = None

        if sort:
            self.sort()
        if build_index:
            self.build_index()

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)

    def __getitem__(self, item):
        return self.tweet_times[item]

    def build_index(self):
        if len(self.tweet_times) is 0:
            return

        last_entry = None
        for i in range(len(self.tweet_times)):
            start_of_day = int(self.tweet_times[i] / 86400) * 86400
            if start_of_day not in self.index:
                self.index[start_of_day] = {'start': i, 'len': 1}
                if last_entry is not None:
                    self.index[last_entry[0]]['len'] = i - last_entry[1]
                last_entry = (start_of_day, i)

        self.index[last_entry[0]]['len'] = len(self.tweet_times) - last_entry[1]
        self.index_keys = self.index.keys()
        self.index_keys.sort()

    def sort(self):
        self.tweet_times.sort()

    def sublist(self, start_date=None, end_date=None):
        """
        :param start_date: The start of time that we want to calculate (should be start of a day)
        :param end_date: The end of time that we want to calculate (should be start of a day)
        :return: a TweetList with tweets in range [start_date, end_date] excluding the end_date itself
        """
        if start_date is None:
            start_date = datetime.datetime.fromtimestamp(0)
        if end_date is None:
            end_date = datetime.datetime.fromtimestamp(max([0] + self.tweet_times[-1]))

        start_unix = unix_timestamp(start_date)
        end_unix = unix_timestamp(end_date)

        start = bisect.bisect_left(self.index_keys, start_unix)  # index in index_keys
        end = bisect.bisect_left(self.index_keys, end_unix)  # index in index_keys

        ending_tweet_index = len(self.tweet_times) if end == len(self.index_keys) \
            else self.index[self.index_keys[end]]['start']

        starting_tweet_index = len(self.tweet_times) if start == len(self.index_keys) \
            else self.index[self.index_keys[start]]['start']

        # todo: check if indexing gets incorrect during sub-listing (needs a shift)
        return TweetList(self.tweet_times[starting_tweet_index:ending_tweet_index], index=self.index, build_index=False)

    def daily_tweets(self, date):
        key = int(unix_timestamp(date) / 86400) * 86400
        if key in self.index:
            return TweetList(
                self.tweet_times[self.index[key]['start']:(self.index[key]['start'] + self.index[key]['len'])])
        else:
            return TweetList([])

    def get_periodic_intensity(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: intensity in the period length (returnes the cached version if possible)
        """

        if self._intensity_cache_conf is not None and \
                        self._intensity_cache_conf['plen'] == period_length and \
                        self._intensity_cache_conf['tslots'] == time_slots:
            return self._intensity_cache
        else:
            self._intensity_cache_conf = {'plen': period_length, 'tslots': time_slots}

        if time_slots is None:
            time_slots = [1.] * period_length

        assert sum(time_slots) == period_length

        tweets_per_slot = [0] * len(time_slots)

        if len(self.tweet_times) is 0:
            intensity = Intensity()
            for i in range(len(time_slots)):
                intensity.append(rate=0., length=time_slots[i])
            return intensity

        total_time = (max(self.tweet_times) - min(self.tweet_times)) / 3600.
        total_number_of_periods = ceil(total_time / period_length)
        if total_number_of_periods == 0:
            total_number_of_periods = 1

        for time in self.tweet_times:
            tweets_per_slot[find_interval(time, period_length, time_slots)] += 1

        intensity = Intensity()

        for i in range(len(time_slots)):
            intensity.append(rate=tweets_per_slot[i] / total_number_of_periods / time_slots[i],
                             length=time_slots[i])

        self._intensity_cache = intensity
        return intensity

    def get_connection_probability(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: probability of being online in each time slot during period length (cache support)
        """
        if self._conn_prob_cache_conf is not None and \
                        self._conn_prob_cache_conf['plen'] == period_length and \
                        self._conn_prob_cache_conf['tslots'] == time_slots:
            return self._conn_prob_cache
        else:
            self._conn_prob_cache_conf = {'plen': period_length, 'tslots': time_slots}

        if time_slots is None:
            time_slots = [1.] * period_length

        assert sum(time_slots) == period_length

        if len(self.tweet_times):
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

            self._conn_prob_cache = [len(bag) / period_count for bag in bags]
        else:
            self._conn_prob_cache = [0.] * len(time_slots)

        return self._conn_prob_cache


def find_interval(tweet_time, period_length, time_slots):
    # With assumption of equal time intervals...
    return int(floor((tweet_time % (period_length * 3600)) / (3600. * time_slots[0])))


def main():
    l = [1168573801, 1168616489, 1168617290, 1168630416,
         1168811197,
         1168837946,
         1168913318, 1168986092,
         1169089667, 1169101612,
         1169183029,
         1169262913, 1169323501,
         1169355915, 1169360554, 1169395149, 1169401647, ]

    pprint([str(x) + ' ' + str(datetime.datetime.fromtimestamp(x)) for x in l])
    t = TweetList(l)
    pprint(t.index)
    print(t.daily_tweets(datetime.datetime.fromtimestamp(1169164800)))
    print(t.sublist(datetime.datetime.fromtimestamp(1168616489), datetime.datetime.fromtimestamp(1169251200)))


if __name__ == '__main__':
    main()
