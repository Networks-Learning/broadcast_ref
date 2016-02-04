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

test_start_date = datetime(2009, 6, 4)
test_end_date = datetime(2009, 9, 3)
total_weeks = int((test_end_date - test_start_date).days / 7)
path_prefix = '/local/moreka/np_data/'


def fetch_wall_array(user, learn_start_date, learn_end_date):
    intensity_arr = np.zeros((len(user.followers()), 7 * 24))
    connection_arr = np.zeros((len(user.followers()), 7 * 24))

    for i in range(len(user.followers())):
        target = user.followers()[i]
        sublist = target.wall_tweet_list().sublist(learn_start_date, learn_end_date)

        intensity_arr[i, :] = sublist.get_periodic_intensity()
        connection_arr[i, :] = sublist.get_connection_probability()

    return intensity_arr, connection_arr


def worker(pid, user_id, num_months_to_learn):
    print '[Process-%d] Worker started for user %d on learning %d months' % (pid, user_id, num_months_to_learn)

    learn_start_date = test_start_date - timedelta(days=num_months_to_learn * 30)
    learn_end_date = test_start_date - timedelta(seconds=1)

    repo = HDFSSQLiteUserRepository(HDFSLoader(), DbConnection())
    user = User(user_id, repo)

    wall_intensity_data, conn_probability_data = fetch_wall_array(user, learn_start_date, learn_end_date)

    np.save('%s%08d_%02d_wall' % (path_prefix, user_id, months), wall_intensity_data)
    np.save('%s%08d_%02d_conn' % (path_prefix, user_id, months), conn_probability_data)

    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_weeks / 7.

    best_intensity = learn_and_optimize(user, budget=budget,
                                        learn_start_date=learn_start_date,
                                        learn_end_date=learn_end_date,
                                        start_hour=0, end_hour=176,
                                        threshold=0.02)

    np.save('%s%08d_%02d_best' % (path_prefix, user_id, months), np.array(best_intensity))

    return


if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.INFO)

    good_users = np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()
    # good_users = [16173435]  #, 33830602, 16648152, 17404514, 6094672, 21010474]

    jobs = []
    for i in range(len(good_users)):
        for months in [3]:
            p = multiprocessing.Process(target=worker, args=(i + 1, good_users[i], months,))
            jobs.append(p)
            p.start()

    for j in jobs:
        j.join()
        sys.stderr.write('%s.exitcode = %s\n' % (j.name, j.exitcode))

