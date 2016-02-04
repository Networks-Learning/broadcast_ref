from __future__ import division
import bisect
import numpy as np
from util.cal import unix_timestamp
from util.decorators import cache_enabled

import pyximport; pyximport.install()
import helper


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

    def _get_tweet_list(self):
        raise NotImplementedError()

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

    @cache_enabled
    def get_periodic_intensity(self, period_length, start_date, end_date):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :return: intensity in the period length (returns the cached version if possible)
        """

        if len(self) is 0:
            return [0.] * period_length

        total_number_of_periods = int(unix_timestamp(end_date) / 3600 / period_length) - \
                                  int(unix_timestamp(start_date) / 3600 / period_length) + 1

        tweets_per_slot = helper.get_intensity_cy(self._get_tweet_list(), period_length)

        return [tps / total_number_of_periods for tps in tweets_per_slot]

    @cache_enabled
    def get_connection_probability(self, period_length, start_date, end_date):
        """
        :param period_length: in hours, default is one week (must be an integer if time_slots is None)
        :return: probability of being online in each time slot during period length (cache support)
        """

        if len(self) is 0:
            return [0.] * period_length

        bags = helper.get_connection_bags_cy(self._get_tweet_list(), period_length)

        period_count = int(unix_timestamp(end_date) / 3600 / period_length) - \
                       int(unix_timestamp(start_date) / 3600 / period_length) + 1

        return [bag / period_count for bag in bags]


class TweetList(ITweetList):
    def __init__(self, times=None):
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

    def get_slice_index_left(self, time_ts):
        return bisect.bisect_left(self._tweet_times, time_ts)

    def get_slice_index_right(self, time_ts):
        return bisect.bisect_right(self._tweet_times, time_ts)

    def _get_tweet_list(self):
        return self._tweet_times


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

    def _get_tweet_list(self):
        return self.tweet_list._get_tweet_list()[self._offset:self._offset + self._size]
