import random
import numpy as np
from utils import *
from data.db_connector import DbConnection
from data.models import TweetList
from operator import truediv
from data.models import *
from data.user import *
import timeit

__author__ = 'Rfun'


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

    sum_of_upper_bounds = sum(upper_bounds)
    real_budget = min(int(budget), int(sum_of_upper_bounds))
    total_time = sum(time_slots)

    while len(generated_points) < real_budget:
        new_choice = random.random() * total_time
        # print new_choice
        position = find_position(new_choice, time_slots)
        if slots[position] + 1 <= upper_bounds[position]:
            slots[position] += 1
            generated_points += [new_choice]
    generated_points.sort()
    generated_points = [x + offset for x in generated_points]
    return TweetList(generated_points)


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
        Tweets = target.wall_tweet_list(self.user_id())
        intensity = Tweets.get_periodic_intensity().sub_intensity(offset, offset + sum(time_slots))
        onlinity_probability = Tweets.get_connection_probability()[offset, offset + sum(time_slots)]
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
    return TweetList(generated_points)


def baseline(user, budget, upper_bounds, method, time_slots=None, offset=0):
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
        weights = [1] * len(time_slots)
    else:
        if method == 'iavm' or method == 'pavm':
            weights = np.array([0] * len(time_slots))
            for target in user.followers():
                print('target %d' %(target.user_id()))
                start = timeit.default_timer()
                tweets = target.wall_tweet_list(user.user_id())
                intensity = tweets.get_periodic_intensity().sub_intensity(offset, offset + total_time)
                stop = timeit.default_timer()
                print('finished fetching for him at %.2f' %(stop-start))
                
                start = timeit.default_timer()
                for i in range(len(intensity.get_as_vector()[0])):
                    if intensity[i]['rate'] < 0.0001:  # escaping from division by zero
                        intensity[i]['rate'] = 0.0001
                if method == 'pavm':
                    onlinity_probability = tweets.get_connection_probability()[offset : offset + total_time]
                    weights += map(truediv, onlinity_probability, intensity.get_as_vector()[0])
                else:
                    weights += map(truediv, [1]*len(intensity), intensity.get_as_vector()[0])
                stop = timeit.default_timer()
                print('finished creating for him at %.2f' %(stop-start))

    print('started sampling')

    while len(generated_points) < real_budget:
        nominated_list = sampling(time_slots, weights, real_budget - len(generated_points))
        for time in nominated_list:
            position = find_position(time, time_slots)
            if slots[position] + 1 <= upper_bounds[position]:
                slots[position] += 1
                generated_points += [time]
    print('finished')
    
    generated_points.sort()
    return generated_points


def main():
    conn = DbConnection()
    # user = User(790728, conn, max_followee_per_follower=100)
    # user = User(21629050, conn, max_followee_per_follower=100)
    user = User(20645419, conn, max_followee_per_follower=500)
    print user._followees

if __name__ == '__main__':
    main()