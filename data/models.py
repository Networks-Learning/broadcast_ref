import numpy as np


class Intensity:
    def __init__(self, size=0):
        self.rates = np.zeros(size)
        self.time_intervals = np.zeros(size)

    def get_weekday_rates(self, weekday):
        """
        Assumes a weekly basis for rates and also equal values for time intervals
        :param weekday: 0 for monday and so on
        :return: the rates on that certain day
        """

        intervals_per_day = 24 * 60 * 60 / self.time_intervals[0]
        return self.rates[weekday * intervals_per_day:(weekday + 1) * intervals_per_day]


class TweetList:
    def __init__(self):
        self.tweet_times = []

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)

    def get_weekly_intensity(self, interval_length_in_seconds=3600):

        week = 60 * 60 * 24 * 7

        total_time = (self.tweet_times[-1] - self.tweet_times[0]).total_seconds()
        total_number_of_weeks = int(total_time / week)

        intensity = Intensity(size=week / interval_length_in_seconds)

        for time in self.tweet_times:
            intensity.time_intervals[find_interval(time, interval_length_in_seconds)] += 1.

        for i in range(len(intensity.time_intervals)):
            intensity.time_intervals[i] /= float(total_number_of_weeks)

        return intensity


def find_interval(tweet_time, dt):
        weekday = tweet_time.weekday()
        t = tweet_time.time()
        seconds = t.second + t.minute * 60 + \
                  t.hour * 60 * 60 + weekday * 60 * 60 * 24
        return int(seconds / dt)