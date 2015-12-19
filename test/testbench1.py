from __future__ import print_function, division
from datetime import timedelta
import numpy as np

from data.models import Intensity
from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k


def show_progressbar(counter, total, message):
    print("\r[%% %.2f] processing %s" % (100. * counter / total, str(message)), end="")


def test_avm(test_start_date, test_end_date, user, test_intensity):
    total_weeks = (test_end_date - test_start_date).days / 7

    now = []
    before = []

    for week in range(int(total_weeks)):
        week_start_day = test_start_date + timedelta(days=week * 7)
        week_start_day_unix = int(week_start_day.strftime('%s'))

        week_end_day = week_start_day + timedelta(days=1)

        print("week number {0}/{2}: {1}".format(week + 1, week_start_day, total_weeks))

        simulated_process = generate_piecewise_constant_poisson_process(Intensity(test_intensity))

        real_process = user.tweet_list().daily_tweets(week_start_day)
        real_process = [(x - week_start_day_unix) / 3600. for x in real_process]

        print("simulated process:")
        print(simulated_process)
        print("real process:")
        print(real_process)

        t_counter = 0
        for target in user.followers():
            t_counter += 1
            show_progressbar(t_counter, len(user.followers()), target.user_id())

            test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).daily_tweets(week_start_day)
            tweet_bags = test_list.get_periodic_intensity(24)

            pi = np.zeros(24)
            for j in range(24):
                if tweet_bags[j]['rate'] > 0:
                    pi[j] = 1.

            # test_list.sort()
            target_wall_no_offset = [(x - week_start_day_unix) / 3600. for x in test_list.tweet_times]

            now.append(time_being_in_top_k(simulated_process, target_wall_no_offset, 1, 24., pi))
            before.append(time_being_in_top_k(real_process, target_wall_no_offset, 1, 24., pi))

        print('')

    print(sum(now), sum(before))
