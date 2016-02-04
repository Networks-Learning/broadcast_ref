from __future__ import division
import logging
import multiprocessing
import numpy as np
import sys
from Queue import Queue

sys.path.append('/local/moreka/broadcast-ref')

from datetime import datetime, timedelta
from data.db_connector import DbConnection
from data.hdfs import HDFSLoader
from data.user import User
from data.user_repo import HDFSSQLiteUserRepository
from opt.optimizer import learn_and_optimize
from util.cal import unix_timestamp
from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k
from competitors.avm import ravm

test_start_date = datetime(2009, 5, 14)
test_end_date = datetime(2009, 8, 13)
test_start_date_unix = unix_timestamp(test_start_date)

total_days = int((test_end_date - test_start_date).days)
in_path_prefix = '/local/moreka/np_data/'
out_path_prefix = '/local/moreka/np_results/'


def fetch_wall_array(user, learn_start_date, learn_end_date):
    intensity_arr = np.zeros((len(user.followers()), 24))
    connection_arr = np.zeros((len(user.followers()), 24))

    for i in range(len(user.followers())):
        target = user.followers()[i]
        print(target.user_id())
        sublist = target.wall_tweet_list().sublist(learn_start_date, learn_end_date)

        intensity_arr[i, :] = sublist.get_periodic_intensity(24, learn_start_date, learn_end_date)
        connection_arr[i, :] = sublist.get_connection_probability(24, learn_start_date, learn_end_date)

    return intensity_arr, connection_arr


def worker(pid, user_id, num_months_to_learn):
    print '[Process-%d] Worker started for user %d on learning %d months' % (pid, user_id, num_months_to_learn)

    learn_start_date = test_start_date - timedelta(days=num_months_to_learn * 30)
    learn_end_date = test_start_date - timedelta(seconds=1)

    repo = HDFSSQLiteUserRepository(HDFSLoader(), DbConnection())
    user = User(user_id, repo)

    wall_intensity_data, conn_probability_data = fetch_wall_array(user, learn_start_date, learn_end_date)
    months = num_months_to_learn

    np.save('%s%08d_%02d_wall' % (in_path_prefix, user_id, months), wall_intensity_data)
    np.save('%s%08d_%02d_conn' % (in_path_prefix, user_id, months), conn_probability_data)

    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_days

    best_intensity, upper_bounds = learn_and_optimize(user, budget=budget,
                                                                learn_start_date=learn_start_date,
                                                                learn_end_date=learn_end_date,
                                                                start_hour=0, end_hour=24,
                                                                period_length=24,
                                                                threshold=0.02)

    np.save('%s%08d_%02d_best' % (in_path_prefix, user_id, months), np.array(best_intensity))

    print("Now let's do the practical work")
    do_practical_test(user, num_months_to_learn, upper_bounds)

    return


def repeated_test(intensity, user, data, n):
    result = []
    for iteration in range(10):
        simulated_process = generate_piecewise_constant_poisson_process(intensity)
        now = []
        for target in user.followers():
            now.append(time_being_in_top_k(simulated_process,
                                            data[target.user_id()]['wall_no_offset'], 1, n,
                                            data[target.user_id()]['pi']))
        result.append(sum(now))
    return np.mean(result)


def do_practical_test(user, month, upper_bounds):
    n = total_days * 24

    real_process = list(user.tweet_list().sublist(test_start_date, test_end_date))
    real_process = [(x - test_start_date_unix) / 3600. for x in real_process]

    data = {}
    before = []
    for target in user.followers():
        print(target.user_id())
        test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(test_start_date, test_end_date)
        target_wall_no_offset = ((test_list._get_tweet_list() - test_start_date_unix) / 3600.).tolist()

        tweet_bags = target.tweet_list().sublist(test_start_date, test_end_date). \
            get_periodic_intensity(n, test_start_date, test_end_date)

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
            np.save('%sbad_%08d_%02d_visibility_avm' % (out_path_prefix, user.user_id(), month), [-1.])
        return

    s_before = sum(before)

    for month in [3]:
        best_intensity = np.tile(np.load('%s%08d_%02d_best.npy' % (in_path_prefix, user.user_id(), month)), total_days)

        budget = sum(best_intensity)
        best_intensity_ravm = np.tile(ravm(budget, upper_bounds), total_days)
        
        print('testing avm')
        res = repeated_test(best_intensity, user, data, n) / s_before
        np.save('%s%08d_%02d_visibility_avm' % (out_path_prefix, user.user_id(), month), [res])
        
        print('testing ravm')
        res = repeated_test(best_intensity_ravm, user, data, n) / s_before
        np.save('%s%08d_%02d_visibility_ravm' % (out_path_prefix, user.user_id(), month), [res])


if __name__ == '__main__':
    multiprocessing.log_to_stderr(logging.INFO)

    good_users = list(set(np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()))
    rand_users = np.random.randint(0, len(good_users), 10)

    pool = multiprocessing.Pool(48)
    results = []
    for i in range(10):
        results.append(pool.apply_async(worker, (i + 1, good_users[rand_users[i]], 3,)))
    
    for i in range(10):
        results[i].get()
