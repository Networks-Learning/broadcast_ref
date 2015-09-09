import numpy as np


class Intensity:
    def __init__(self, size):
        self.rates = np.array(size)
        self.time_intervals = np.array(size)


class TweetList:
    def __init__(self):
        self.tweet_times = []

    def __str__(self):
        return '%d tweets: ' % len(self.tweet_times) + str(self.tweet_times)