from __future__ import division
import bisect
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
    def __init__(self, times=None, build_index=True, sort=True, index=None):
        """
        Time is in unix timestamp format, and so is in seconds precision
        :param times: time list, can be none, if ndarray remains the same
        """
        self.tweet_times = [] if times is None else np.array(times)
        self.index = {} if (build_index is False or index is None) else index

        if sort:
            self.sort()
        if build_index:
            self.build_index()

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)

    def __getitem__(self, item):
        return self.tweet_times[item]

    def build_index(self):
        prev_i, prev_key = 0, 0
        for i in range(len(self.tweet_times)):
            start_of_day = int(self.tweet_times[i] / 86400) * 86400
            if start_of_day not in self.index.keys():
                self.index[start_of_day] = i

    def sort(self):
        self.tweet_times.sort()

    def sublist(self, start_date=None, end_date=None):
        """
        :param start_date: The start of time that we want to calculate
        :param end_date: The end of time that we want to calculate
        :return: a TweetList with tweets in range [start_time, end_time]
        """
        if start_date is None:
            start_date = datetime.datetime.fromtimestamp(0)
        if end_date is None:
            end_date = datetime.datetime.fromtimestamp(max([0] + self.tweet_times[-1]))

        start = bisect.bisect_left(self.index.keys(), long(start_date.strftime('%s')))
        end = bisect.bisect_right(self.index.keys(), long(end_date.strftime('%s')))

        # todo: indexing gets incorrect during sub-listing (needs a shift)
        # todo: index[start]:index[end]?
        return TweetList(self.tweet_times[start:end], index=self.index)

    def daily_tweets(self, date):
        key = int(long(date.strftime('%s')) / 86400) * 86400
        if key in self.index:
            return

    def get_periodic_intensity(self, period_length=24 * 7, time_slots=None):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :param time_slots: if not provided, default is equal time slots of one hour
        :return: intensity in the period length
        """
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

            return [len(bag) / period_count for bag in bags]

        else:
            return [0.] * len(time_slots)


def find_interval(tweet_time, period_length, time_slots):
    # With assumption of equal time intervals...
    return int(floor((tweet_time % (period_length * 3600)) / (3600. * time_slots[0])))


def main():
    t = TweetList([])
    t.sublist(datetime.datetime(2007, 10, 1), datetime.datetime.now())
    t.get_connection_probability()
    t.get_periodic_intensity()


if __name__ == '__main__':
    main()
