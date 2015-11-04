from __future__ import division
import random
import sys
from data import models
import numpy as np


class User:
    _user_id = None
    _tweet_list = None
    _intensity = None
    _probability = None
    _wall_tweet_list = None
    _wall_intensity = None
    _followees = None
    _followers = None
    _followers_weights = None
    _conn = None

    options = {}

    def __init__(self, user_id, conn, **kwargs):
        self._conn = conn
        self._user_id = user_id

        self.options['period_length'] = 24 * 7
        self.options['time_slots'] = [1.] * (24 * 7)
        self.options['top_k'] = 10
        self.options['max_followee_per_follower'] = 500

        for k in kwargs:
            self.options[k] = kwargs[k]

    def __str__(self):
        return 'User[%d]' % self._user_id

    def __repr__(self):
        return self.__str__()

    def user_id(self):
        return self._user_id

    def tweet_list(self):
        if self._tweet_list is not None:
            return self._tweet_list

        cur = self._conn.get_cursor()
        tweet_times = cur.execute('select tweet_time from db.tweets where user_id=?', (self._user_id,)).fetchall()
        cur.close()

        self._tweet_list = models.TweetList([t[0] for t in tweet_times])
        return self._tweet_list

    def followees(self):
        if self._followees is not None:
            return self._followees

        self._followees = []

        cur = self._conn.get_cursor()
        followees = cur.execute('select idb from li.links where ida=?', (self._user_id,)).fetchall()
        cur.close()

        for followee in followees:
            followee_id = followee[0]
            followee_user = User(followee_id, self._conn, **self.options)
            self._followees.append(followee_user)

        return self._followees

    def followers(self):
        if self._followers is not None:
            return self._followers

        self._followers = []

        cur = self._conn.get_cursor()
        followers = cur.execute('select ida from li.links where idb=?', (self._user_id,)).fetchall()
        cur.close()

        for follower in followers:
            follower_id = follower[0]
            follower_user = User(follower_id, self._conn, **self.options)

            follower_followee_count = len(follower_user.followees())
            if follower_followee_count <= self.options['max_followee_per_follower']:
                self._followers.append(follower_user)
            else:
                sys.stderr.write('Dropped user %d, because he had %d followers!\n' %
                                 (follower_user.user_id(), follower_followee_count))
                del follower_user

        return self._followers

    def set_follower_weight(self, follower, weight):
        if self._followers_weights is None:
            self.followers_weights()

        if isinstance(follower, User):
            self._followers_weights[follower.user_id()] = weight
        else:
            self._followers_weights[follower] = weight

    def get_follower_weight(self, follower):
        if self._followers_weights is None:
            self.followers_weights()

        if isinstance(follower, User):
            return self._followers_weights[follower.user_id()]
        else:
            return self._followers_weights[follower]

    def followers_weights(self):
        if self._followers_weights is not None:
            return self._followers_weights

        self._followers_weights = {}

        follower_count = len(self.followers())
        for follower in self.followers():
            self._followers_weights[follower.user_id()] = 1. / follower_count

        return self._followers_weights

    def wall_tweet_list(self, excluded_user_id=0):
        if self._wall_tweet_list is not None:
            return self._wall_tweet_list

        sys.stderr.write('fetching wall tweet list for %d\n' % self.user_id())

        cur = self._conn.get_cursor()
        tweets = cur.execute(
            'SELECT tweet_time FROM db.tweets WHERE user_id IN (SELECT idb FROM li.links WHERE ida=? AND idb != ?)',
            (self.user_id(), excluded_user_id)).fetchall()
        cur.close()
        sys.stderr.write('fetch done\n')
        self._wall_tweet_list = models.TweetList([tweet[0] for tweet in tweets])
        return self._wall_tweet_list

    def find_position(self, time, time_slots):
        start_point = 0
        for i in range(len(time_slots)):
            if start_point <= time < start_point + time_slots[i]:
                return i
            else:
                start_point += time_slots[i]
        return -1

    def ravm(self, budget, upper_bounds, time_slots=None, offset=0):
        """
        :param budget: total budget that we want to pay
        :param upper_bounds: upper bounds for number of event in each slot
        :param time_slots: time intervals, if nothing was passed we assume each time interval to be 1 hour
                time slots is relative to the weeks,for example time slots with value 34 is 6 A.M in Friday
        :return: returns a list of event times, started from zero
        """
        if time_slots is None:
            time_slots = [1] * (len(upper_bounds))
        generated_points = []
        slots = [0] * len(time_slots)
        assert len(time_slots) == len(upper_bounds)

        while len(generated_points) < int(budget) and len(generated_points) < int(sum(upper_bounds)):
            new_choice = random.random() * sum(time_slots)
            # print new_choice
            position = self.find_position(new_choice, time_slots)
            if slots[position] + 1 <= upper_bounds[position]:
                slots[position] += 1
                generated_points += [new_choice]
        generated_points.sort()
        generated_points = [x + offset for x in generated_points]
        return models.TweetList(generated_points)

    def gavm(self, budget, upper_bounds, time_slots=None, offset=0):
        """
        :param budget: total budget that we want to pay
        :param upper_bounds: upper bounds for number of event in each slot
        :param time_slots: time intervals, if nothing was passed we assume each time interval to be 1 hour
                time slots is relative to the weeks,for example time slots with value 34 is 6 A.M in Friday
        :return: returns a list of event times, started from zero
        """

        if time_slots is None:
            time_slots = [1] * (len(upper_bounds))
        generated_points = []
        slots = [0] * len(time_slots)
        utility = np.array([0] * len(time_slots))
        assert len(time_slots) == len(upper_bounds)

        for target in self.followers():
            tweets = target.wall_tweet_list(self.user_id())
            intensity = tweets.get_periodic_intensity().sub_intensity(offset, offset + sum(time_slots))
            onlinity_probability = tweets.get_connection_probability()[offset, offset + sum(time_slots)]
            for i in range(len(intensity)):
                if intensity[i]['rate'] < 0.0001:  # escaping from division by zero
                    intensity[i]['rate'] = 0.0001
                    utility[i] += onlinity_probability / intensity[i]['rate']

        while len(generated_points) < int(budget) and len(generated_points) < int(sum(upper_bounds)):
            m = utility.argmax()
            while slots[m] < int(upper_bounds[m]) and len(generated_points) < int(budget):
                new_point = random.random() * time_slots[m] + sum(time_slots[0:m - 1]) + offset
                generated_points += [new_point]
                slots[m] += 1
            utility[m] = -1

        generated_points.sort()
        return models.TweetList(generated_points)
