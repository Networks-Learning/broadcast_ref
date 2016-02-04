from __future__ import division

import logging
import multiprocessing
import numpy as np
import sys

sys.path.append('/local/moreka/broadcast-ref')

from datetime import datetime, timedelta

from data.db_connector import DbConnection
from data.hdfs import HDFSLoader
from data.user import User
from data.user_repo import HDFSSQLiteUserRepository
from opt.optimizer import learn_and_optimize
from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k
from util.cal import unix_timestamp

test_start_date = datetime(2009, 6, 4)
test_start_date_unix = unix_timestamp(test_start_date)
test_end_date = datetime(2009, 9, 3)
total_weeks = int((test_end_date - test_start_date).days / 7)

in_path_prefix = '/local/moreka/np_data/'
out_path_prefix = '/local/moreka/np_results/'

THEORETICAL = 1
SIMULATION = 2
PRACTICAL = 3


def do_theoretical_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn):
    pass


def do_simulation_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn):
    pass


def do_practical_work(pid, user_id, month_list):
    repo = HDFSSQLiteUserRepository(HDFSLoader(), DbConnection())
    user = User(user_id, repo)

    n = total_weeks * 7 * 24

    result = []

    real_process = list(user.tweet_list().sublist(test_start_date, test_end_date))
    real_process = [(x - test_start_date_unix) / 3600. for x in real_process]
    if any(real_process[i+1] < real_process[i] for i in range(len(real_process) - 1)):
        print('the problem is in real process of user %d' % user_id)

    data = {}
    before = []
    for target in user.followers():
        print('fetching %d ...' % target.user_id())
        test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(test_start_date, test_end_date)
        target_wall_no_offset = ((test_list._get_tweet_list() - test_start_date_unix) / 3600.).tolist()

        tweet_bags = target.tweet_list().sublist(test_start_date, test_end_date).get_periodic_intensity(n)
        pi = np.zeros(n)
        for j in range(n):
            if tweet_bags[j] > 0:
                pi[j] = 1.

        data[target.user_id()] = {
            'wall_no_offset': target_wall_no_offset,
            'pi': pi
        }
        before.append(time_being_in_top_k(real_process, target_wall_no_offset, 1, n, pi))

    if sum(before) == 0.:
        for month in [3]:
            np.save('%sbad_%08d_%02d_visibility_avm' % (out_path_prefix, user_id, month), [-1.])
        return
    
    s_before = sum(before)

    for month in [3]:
        best_intensity = np.tile(np.load('%s%08d_%02d_best.npy' % (in_path_prefix, user_id, month)), total_weeks * 7)  # TODO: clean this shit

        for iteration in range(10):
            simulated_process = generate_piecewise_constant_poisson_process(best_intensity)
            if any(x < 0 for x in real_process):
                print('the problem is in simulated process')
            # print(simulated_process)
            now = []
            for target in user.followers():
                now.append(
                    time_being_in_top_k(simulated_process,
                                        data[target.user_id()]['wall_no_offset'], 1, n,
                                        data[target.user_id()]['pi']))

            result.append(sum(now) / s_before)

#         print('RESULT is')
#         print(result)
        np.save('%s%08d_%02d_visibility_avm' % (out_path_prefix, user_id, month), [np.mean(result)])

    return


def worker(pid, user_id, num_months_to_learn, work):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user_id, num_months_to_learn))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user_id, num_months_to_learn))
    best_intensity = np.load('%s%08d_%02d_best.npy' % (in_path_prefix, user_id, num_months_to_learn))

    if work is THEORETICAL:
        do_theoretical_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn)
    elif work is SIMULATION:
        do_simulation_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn)

    return


if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.INFO)

    good_users = list(set(np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()))
#     good_users = [33830602]  #, 33830602, 16648152, 17404514, 6094672, 21010474]

    jobs = []
    for i in range(len(good_users)):
        # for months in [3, 6, 9]:
        #     for work in [SIMULATION, THEORETICAL]:
        #         p = multiprocessing.Process(target=worker, args=(i + 1, good_users[i], months, work))
        #         jobs.append(p)
        #         p.start()

        p = multiprocessing.Process(target=do_practical_work, args=(i + 1, good_users[i], [3, 6, 9]))
        jobs.append(p)
        p.start()

    for j in jobs:
        j.join()
        sys.stderr.write('%s.exitcode = %s\n' % (j.name, j.exitcode))
