from __future__ import division
import random
import sys
from data import models
import numpy as np

from util.decorators import cache_enabled


class User:
    _user_id = None
    _repo = None
    _followers_weights = None

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

    @cache_enabled
    def tweet_list(self):
        return models.TweetList(self._repo.get_user_tweets(self.user_id()))

    @cache_enabled
    def followees(self):
        followees = []

        for followee in self._repo.get_user_followees(self.user_id()):
            # TODO: There are users in Links table that have no tweets and their ID is bigger than 70000000
            if followee >= 70000000:
                continue

            followee_user = User(followee, self._repo, **self.options)
            followees.append(followee_user)

        return followees

    @cache_enabled
    def followers(self):
        followers = []

        for follower in self._repo.get_user_followers(self.user_id()):
            # TODO: There are users in Links table that have no tweets and their ID is bigger than 70000000
            if follower >= 70000000:
                continue

            follower_user = User(follower, self._repo, **self.options)

            follower_followee_count = len(follower_user.followees())
            if follower_followee_count <= self.options['max_followee_per_follower']:
                followers.append(follower_user)
            else:
                del follower_user

        return followers

    def set_follower_weight(self, follower, weight):
        self.followers_weights()

        if isinstance(follower, User):
            self._followers_weights[follower.user_id()] = weight
        else:
            self._followers_weights[follower] = weight

    def get_follower_weight(self, follower):
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

    @cache_enabled
    def wall_tweet_list(self, excluded_user_id=0):
        wall = self._repo.get_user_wall(self.user_id(), excluded_user_id)
        return models.TweetList(wall)
