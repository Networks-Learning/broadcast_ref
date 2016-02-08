from __future__ import print_function, division
from datetime import timedelta
import time

import numpy as np

from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k
from util.cal import unix_timestamp


def show_progressbar(counter, total, message):
    print("\r[%% %.2f] processing %s" % (100. * counter / total, str(message)), end="")


def test_avm(test_start_date, test_end_date, user, test_intensity, iterations=1):
    total_weeks = int((test_end_date - test_start_date).days / 7)

    pi_cache = {}
    data = []

    for iteration in range(iterations):
        t = time.time()
        print('Iteration %d' % (iteration + 1))
        now = []
        before = []
        for week in range(int(total_weeks)):
            week_start_day = test_start_date + timedelta(days=week * 7)
            week_start_day_unix = unix_timestamp(week_start_day)

            print("\r  [week number {0}/{2}: {1}]".format(week + 1, week_start_day, total_weeks), end='')

            simulated_process = generate_piecewise_constant_poisson_process(test_intensity)

            real_process = user.tweet_list().get_day_tweets(week_start_day)
            real_process = [(x - week_start_day_unix) / 3600. for x in real_process]

            # print("--> simulated process:")
            # pprint(simulated_process)
            # print("--> real process:")
            # pprint(real_process)

            t_counter = 0
            for target in user.followers():
                t_counter += 1
                # show_progressbar(t_counter, len(user.followers()), target.user_id())

                test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).get_day_tweets(week_start_day)

                if target.user_id() in pi_cache:
                    pi = pi_cache[target.user_id()]
                else:
                    tweet_bags = test_list.get_periodic_intensity(24, test_start_date, test_end_date)

                    pi = np.zeros(24)
                    for j in range(24):
                        if tweet_bags[j] > 0:
                            pi[j] = 1.
                    pi_cache[target.user_id()] = pi

                target_wall_no_offset = [(x - week_start_day_unix) / 3600. for x in test_list]

                now.append(time_being_in_top_k(simulated_process, target_wall_no_offset, 1, 24., pi))
                before.append(time_being_in_top_k(real_process, target_wall_no_offset, 1, 24., pi))

        data.append((now, before))
        print('\n Took %d seconds' % (time.time() - t))

    return data

