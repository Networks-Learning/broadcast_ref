from __future__ import division
from competitors.utils import find_position, sampling
from operator import truediv
import numpy as np
import timeit

__author__ = 'Rfun'

METHOD_RAVM = 0
METHOD_IAVM = 1
METHOD_PAVM = 2


def baseline(user, budget, upper_bounds, method, time_slots=None, offset=0):
    """
    :param user: Us
    :type user: User
    :param method: one of mentioned methods
    :param offset: offset to the generated tweet_list to match the real times of tweets
    :return:
    """

    if time_slots is None:
        time_slots = [1] * (len(upper_bounds))

    generated_points = []

    slots = [0] * len(time_slots)
    assert len(time_slots) == len(upper_bounds)

    sum_of_upper_bounds = sum(upper_bounds)
    real_budget = min(int(budget), int(sum_of_upper_bounds))
    total_time = sum(time_slots)

    weights = [0] * len(time_slots)
    if method == 'ravm':
        weights = np.array([0] * len(time_slots))
    else:
        if method == 'iavm' or method == 'pavm':
            weights = np.array([0.] * len(time_slots))
            for target in user.followers():
                print 'started fetching for %d with %d followee' % (target.user_id(), len(target.followees()))
                start = timeit.default_timer()
                tweets = target.wall_tweet_list(user.user_id())
                stop = timeit.default_timer()
                print '%.2f time for fetching' % (stop - start)
                print 'tweets: ', tweets
                p_intensity = tweets.get_periodic_intensity()
                intensity = p_intensity.sub_intensity(offset, offset + total_time)

                for i in range(len(intensity.get_as_vector()[0])):
                    if intensity[i]['rate'] < 0.0001:  # escaping from division by zero
                        intensity[i]['rate'] = 0.0001
                if method == 'pavm':
                    onlinity_probability = target.tweet_list().get_connection_probability()[offset: offset + total_time]
                    weights += map(truediv, onlinity_probability, intensity.get_as_vector()[0])
                else:
                    weights += map(truediv, [1] * len(intensity), intensity.get_as_vector()[0])
    while len(generated_points) < real_budget:
        nominated_list = sampling(time_slots, weights, real_budget - len(generated_points))
        for time in nominated_list:
            position = find_position(time, time_slots)
            if slots[position] + 1 <= upper_bounds[position]:
                slots[position] += 1
                generated_points += [time]

    generated_points.sort()
    return generated_points


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