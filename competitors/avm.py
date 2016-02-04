from __future__ import division
from competitors.utils import find_position, sampling
from operator import truediv
import numpy as np
import timeit

__author__ = 'Rfun'

METHOD_RAVM = 0
METHOD_IAVM = 1
METHOD_PAVM = 2


def ravm(budget, upper_bounds):

    budget = min(budget, sum(upper_bounds))
    remained_budget = budget

    result = np.zeros(len(upper_bounds))

    while remained_budget > 1e-6:

        share = remained_budget / (len(upper_bounds))
        for i in range(len(upper_bounds)):
            if upper_bounds[i] - result[i] > 1e-6:
                temp = min(share, upper_bounds[i] - result[i])
                result[i] += temp
                remained_budget -= temp

    return result


def iavm(budget, upper_bounds, user, test_start_date, test_end_date):
    print('iavm')
    budget = min(budget, sum(upper_bounds))
    remained_budget = budget
    result = np.zeros(len(upper_bounds))
    weights = np.zeros(len(upper_bounds))
    probability = np.ones(len(upper_bounds))
    
    for follower in user.followers():
        follower_wall_intensity = np.array(follower.wall_tweet_list().sublist(test_start_date, test_end_date).\
        get_periodic_intensity()[:len(upper_bounds)])
        weights += np.array([probability[i]/max(follower_wall_intensity[i], 1e-6) for i in range(len(upper_bounds))])
        
    weights = weights*(1/sum(weights))
    
    while remained_budget > 1e-6:

        shares = remained_budget * weights
        for i in range(len(upper_bounds)):
            if upper_bounds[i] - result[i] > 1e-6:
                temp = min(shares[i], upper_bounds[i] - result[i])
                result[i] += temp
                remained_budget -= temp

    return result

def pavm(budget, upper_bounds, user, test_start_date, test_end_date):
    print('pavm')
    budget = min(budget, sum(upper_bounds))
    remained_budget = budget
    result = np.zeros(len(upper_bounds))
    weights = np.zeros(len(upper_bounds))
    
    followers_wall_intensities = np.zeros(len(upper_bounds))
    
    for follower in user.followers():
        follower_wall_intensity = np.array(follower.wall_tweet_list().sublist(test_start_date, test_end_date).\
        get_periodic_intensity()[:len(upper_bounds)])
        follower_connection_probability = follower.tweet_list().sublist(test_start_date, test_end_date).\
            get_connection_probability()[:len(upper_bounds)]
        weights += np.array([follower_connection_probability[i]/max(follower_wall_intensity[i],1e-6) \
                             for i in range(len(upper_bounds))])
        
    weights = weights*(1/sum(weights))
    
    while remained_budget > 1e-6:

        shares = remained_budget * weights
        for i in range(len(upper_bounds)):
            if upper_bounds[i] - result[i] > 1e-6:
                temp = min(shares[i], upper_bounds[i] - result[i])
                result[i] += temp
                remained_budget -= temp

    return result

def main():
    pass


if __name__ == '__main__':
    main()