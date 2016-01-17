from competitors.utils import find_position, sampling
from data.db_connector import DbConnection
from operator import truediv
from data.models import *
from data.user import *

__author__ = 'Rfun'


def baseline(self, user, budget, upper_bounds, method, time_slots=None, offset=0):
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
                tweets = target.wall_tweet_list(user.user_id())
                intensity = tweets.get_periodic_intensity().sub_intensity(offset, offset + total_time)

                for i in range(len(intensity)):
                    if intensity[i]['rate'] < 0.0001:  # escaping from division by zero
                        intensity[i]['rate'] = 0.0001
                if method == 'pavm':
                    onlinity_probability = tweets.get_connection_probability()[offset, offset + total_time]
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


def main():
    conn = DbConnection()
    # user = User(790728, conn, max_followee_per_follower=100)
    # user = User(21629050, conn, max_followee_per_follower=100)
    # user = User(20645419, conn, max_followee_per_follower=500)
    # print user._followees
    pass

if __name__ == '__main__':
    main()