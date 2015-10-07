from __future__ import division
import sys
from data import models

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
        tweet_times = cur.execute('select tweet_time from tweets where user_id=?', (self._user_id,)).fetchall()
        cur.close()

        self._tweet_list = models.TweetList([t[0] for t in tweet_times])
        return self._tweet_list

    def followees(self):
        if self._followees is not None:
            return self._followees

        self._followees = []

        cur = self._conn.get_cursor()
        followees = cur.execute('select idb from links where ida=?', (self._user_id,)).fetchall()
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
        followers = cur.execute('select ida from links where idb=?', (self._user_id,)).fetchall()
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
            'SELECT tweet_time FROM tweets WHERE user_id IN (SELECT idb FROM links WHERE ida=? AND idb != ?)',
            (self.user_id(), excluded_user_id)).fetchall()
        cur.close()

        self._wall_tweet_list = models.TweetList([tweet[0] for tweet in tweets])
        return self._wall_tweet_list
