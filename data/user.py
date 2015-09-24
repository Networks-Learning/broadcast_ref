from __future__ import division
from data import models
from data.models import Intensity
from opt import optimizer, utils
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
        self.options['top_k'] = 15
        self.options['learn_date_start'] = None
        self.options['learn_date_end'] = None

        for k in kwargs:
            self.options[k] = kwargs[k]

    def __str__(self):
        return str(self._user_id)

    def __repr__(self):
        return self.__str__()

    def user_id(self):
        return self._user_id

    def tweet_list(self):
        if self._tweet_list is not None:
            return self._tweet_list

        cur = self._conn.get_cursor()
        tweet_times = cur.execute('select tweet_time from tweets where user_id=?', (self._user_id,)).fetchall()
        cur.close()

        self._tweet_list = models.TweetList([t[0] for t in tweet_times])
        return self._tweet_list

    def intensity(self):
        if self._intensity is not None:
            return self._intensity

        self._intensity = self.tweet_list().get_periodic_intensity(
            self.options['period_length'], self.options['time_slots'])

        return self._intensity

    def partially_learned_intensity(self, start_time, end_time):
        return self.tweet_list().get_periodic_intensity(self.options['period_length'], self.options['time_slots'],
                                                        start_time, end_time)

    def connection_probability(self):
        if self._probability is not None:
            return self._probability

        self._probability = self.tweet_list().get_connection_probability(
            self.options['period_length'], self.options['time_slots'])

        return self._probability

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

        follower_count = len(followers)

        for follower in followers:
            follower_id = follower[0]
            follower_user = User(follower_id, self._conn, **self.options)
            follower_followee_count = len(follower_user.followees())
            if follower_followee_count <= 500:
                self._followers.append(follower_user)
            else:
                print 'droped user %d, because he had %d followers!' % (follower_user.user_id(), follower_followee_count)
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
        print 'query begin.. for %d' % self.user_id()
        cur = self._conn.get_cursor()
        tweets = cur.execute(
            'SELECT tweet_time FROM tweets WHERE user_id IN (SELECT idb FROM li.links WHERE ida=? AND idb != ?)', (self.user_id(), excluded_user_id)).fetchall()
        cur.close()
#         print 'end query..'
        self._wall_tweet_list = models.TweetList([tweet[0] for tweet in tweets])
#         print 'done!'
        return self._wall_tweet_list

    def wall_intensity(self, excluded_user_id=0):
        if self._wall_intensity is not None:
            return self._wall_intensity

        t_list = self.wall_tweet_list(excluded_user_id)
        self._wall_intensity = t_list.get_periodic_intensity(self.options['period_length'], self.options['time_slots'])

        return self._wall_intensity

    def optimize(self, budget=None, upper_bounds=None, start_hour=0, end_hour=24,
                 util=utils.weighted_top_one, util_gradient=utils.weighted_top_one_grad):

        oi = self.intensity().sub_intensity(start_hour, end_hour)
        if budget is None:
            budget = sum([x['rate'] * x['length'] for x in oi])

        if upper_bounds is None:
            upper_bounds = np.zeros(oi.size())
            for target in self.followers():
                ti = target.wall_intensity().sub_intensity(start_hour, end_hour)
                _max = max([oi[i]['rate'] / ti[i]['rate'] for i in range(oi.size()) if ti[i]['rate'] != 0.0])
                upper_bounds += self.get_follower_weight(target) * _max * \
                                np.array([ti[i]['rate'] for i in range(oi.size())])

        followers_intensities = [
            target.wall_intensity().sub_intensity(start_hour, end_hour)
            for target in self.followers()
        ]

        followers_weights = [
            self.get_follower_weight(target)
            for target in self.followers()
        ]

        followers_conn_prob = [
            target.connection_probability()[start_hour:end_hour]
            for target in self.followers()
        ]
        
        yield followers_intensities, followers_weights, followers_conn_prob, upper_bounds

        def _util(x):
            return util(Intensity(x), followers_intensities, followers_conn_prob, followers_weights)

        def _util_grad(x):
            return util_gradient(Intensity(x), followers_intensities, followers_conn_prob, followers_weights)

        yield optimizer.optimize(_util, _util_grad, budget, upper_bounds, threshold=0.005)
        return
