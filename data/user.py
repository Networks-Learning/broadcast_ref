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
    _excluded_user_in_wall = None
    _followees = None
    _followers = None
    _followers_weights = None
    _repo = None

    options = {}

    def __init__(self, user_id, repo, **kwargs):
        """
        :param user_id:
        :type repo: data.user_repo.UserRepository
        :param kwargs:
        :return:
        """
        self._repo = repo
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

        self._tweet_list = models.TweetList(self._repo.get_user_tweets(self.user_id()))
        return self._tweet_list

    def followees(self):
        if self._followees is not None:
            return self._followees

        self._followees = []

        for followee in self._repo.get_user_followees(self.user_id()):
            followee_user = User(followee, self._repo, **self.options)
            self._followees.append(followee_user)

        return self._followees

    def followers(self):
        if self._followers is not None:
            return self._followers

        self._followers = []

        for follower in self._repo.get_user_followers(self.user_id()):
            follower_user = User(follower, self._repo, **self.options)

            follower_followee_count = len(follower_user.followees())
            if follower_followee_count <= self.options['max_followee_per_follower']:
                self._followers.append(follower_user)
            else:
                # sys.stderr.write('Dropped user %d, because he had %d followers!\n' %
                #                 (follower_user.user_id(), follower_followee_count))
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
        if self._wall_tweet_list is not None and excluded_user_id == self._excluded_user_in_wall:
            return self._wall_tweet_list

        self._wall_tweet_list = models.TweetList(self._repo.get_user_wall(self.user_id(), excluded_user_id))
        self._excluded_user_in_wall = excluded_user_id
        return self._wall_tweet_list
