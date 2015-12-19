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

        start_unix = long(start_date.strftime('%s'))
        end_unix = long(end_date.strftime('%s'))

        start = bisect.bisect_left(self.index_keys, start_unix)  # index in index_keys
        end = bisect.bisect_left(self.index_keys, end_unix)  # index in index_keys

        ending_tweet_index = len(self.tweet_times) if end == len(self.index_keys) \
            else self.index[self.index_keys[end]]['start']

        starting_tweet_index = len(self.tweet_times) if start == len(self.index_keys) \
            else self.index[self.index_keys[start]]['start']

        # todo: check if indexing gets incorrect during sub-listing (needs a shift)
        return TweetList(self.tweet_times[starting_tweet_index:ending_tweet_index], index=self.index, build_index=False)

    def daily_tweets(self, date):
        key = int(long(date.strftime('%s')) / 86400) * 86400
        if key in self.index:
            return self.tweet_times[self.index[key]['start']:(self.index[key]['start'] + self.index[key]['len'])]
        else:
            return []

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
    l = [1143955530, 1168573801, 1168616489, 1168617290, 1168630416, 1168811197, 1168837946, 1168913318, 1168986092,
         1169089667, 1169101612, 1169183029, 1169262913, 1169323501, 1169355915, 1169360554, 1169395149, 1169401647,
         1169412392, 1169510487, 1169525445, 1169532512, 1169591032, 1169611957, 1169695044, 1169793982, 1169869433,
         1169973122, 1170111122, 1170112566, 1170120511, 1170131807, 1170186462, 1170194998, 1170195001, 1170205092,
         1170216100, 1170216246, 1170219415, 1170258857, 1170264114, 1170346569, 1170356313, 1170368897, 1170389189,
         1170395895, 1170470294, 1170525531, 1170583180, 1170639166, 1170709433, 1170800674, 1170818361, 1170860529,
         1170873813, 1170906138, 1170910582, 1170986224, 1170996313, 1171064070, 1171067339, 1171069140, 1171149756,
         1171250710, 1171293909, 1171304407, 1171311050, 1171399689, 1171405750, 1171414598, 1171560593, 1171567380,
         1171692832, 1171745636, 1171786117, 1171921200, 1171930491, 1171982756, 1172113730, 1172160786, 1172188347,
         1172550404, 1172604313, 1172631068, 1172632947, 1172683102, 1172713574, 1172714861, 1172717533, 1172764440,
         1172812144, 1172862407, 1172880904, 1173035628, 1173060056, 1173109065, 1173152479, 1173154837, 1173195789,
         1173323687, 1173325612, 1173328733, 1173369118, 1173387226, 1173403138, 1173419045, 1173420755, 1173462644,
         1173464859, 1173484438, 1173491253, 1173498948, 1173532441, 1173540136, 1173541249, 1173559397, 1173565713,
         1173568467, 1173579891, 1173584225, 1173636365, 1173644118, 1173645881, 1173657770, 1173658862, 1173660408,
         1173666805, 1173674193, 1173676542, 1173711723, 1173724698, 1173734349, 1173736124, 1173741724, 1173749580,
         1173755712, 1173756120, 1173797053, 1173806898, 1173815479, 1173819876, 1173837075, 1173855248, 1173911191,
         1173911838, 1173937453, 1173969171, 1173990738, 1174021747, 1174022442, 1174060090, 1174091126, 1174104391]
    t = TweetList(l)
    print(t.index)
    print(t.daily_tweets(datetime.datetime.fromtimestamp(1171745636)))
    print(t.sublist(datetime.datetime.fromtimestamp(1143955530), datetime.datetime.fromtimestamp(1143955530)))


if __name__ == '__main__':
    main()
