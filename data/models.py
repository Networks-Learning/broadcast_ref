import numpy as np


class Intensity:
    def __init__(self, size=0):
        self.rates = np.zeros(size)
        self.time_intervals = np.zeros(size)


class TweetList:
    def __init__(self):
        self.tweet_times = []

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)