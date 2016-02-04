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
from util.cal import unix_timestamp
from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k
from competitors.avm import ravm

test_start_date = datetime(2009, 5, 14)
test_end_date = datetime(2009, 8, 13)
test_start_date_unix = unix_timestamp(test_start_date)

total_days = int((test_end_date - test_start_date).days)
n = total_days * 24

learn_start_date = test_start_date - timedelta(days=3 * 30)  # TODO: suppose month is 3
learn_end_date = test_start_date - timedelta(seconds=1)

in_path_prefix = '/local/moreka/np_data/'
out_path_prefix = '/local/moreka/np_results/'


def fetch_wall_array(user):
    intensity_arr = np.zeros((len(user.followers()), 24))
    connection_arr = np.zeros((len(user.followers()), 24))

    for i in range(len(user.followers())):
        target = user.followers()[i]
        print(target.user_id())
        sublist = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(learn_start_date, learn_end_date)

        intensity_arr[i, :] = sublist.get_periodic_intensity(24, learn_start_date, learn_end_date)
        connection_arr[i, :] = sublist.get_connection_probability(24, learn_start_date, learn_end_date)

    return intensity_arr, connection_arr


def worker(pid, user_id, month=3):
    print '[Process-%d] Worker started for user %d' % (pid, user_id)

    repo = HDFSSQLiteUserRepository(HDFSLoader(), DbConnection())
    user = User(user_id, repo)

    # TEST MODEL 1 #
    fetch_and_save_wall_and_conn(user, month)
    do_practical_test(user, month)

    # TEST MODEL 2 #
    do_theoretical_test(user, month)


def do_theoretical_test(user, month):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))
    best_intensity_avm = np.load('%s%08d_%02d_best_avm.npy' % (in_path_prefix, user.user_id(), month))




def fetch_and_save_wall_and_conn(user, month):
    wall_intensity_data, conn_probability_data = fetch_wall_array(user)

    np.save('%s%08d_%02d_wall' % (in_path_prefix, user.user_id(), month), wall_intensity_data)
    np.save('%s%08d_%02d_conn' % (in_path_prefix, user.user_id(), month), conn_probability_data)

    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_days
    best_intensity, upper_bounds = learn_and_optimize(user, budget=budget,
                                                      learn_start_date=learn_start_date,
                                                      learn_end_date=learn_end_date,
                                                      start_hour=0, end_hour=24,
                                                      period_length=24,
                                                      threshold=0.02)

    np.save('%s%08d_%02d_best_avm' % (in_path_prefix, user.user_id(), month), np.array(best_intensity))

    best_ravm = ravm(budget, upper_bounds)
    np.save('%s%08d_%02d_best_ravm' % (in_path_prefix, user.user_id(), month), best_ravm)

    return upper_bounds


def repeated_test(intensity, user, data):
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


def do_practical_test(user, month):
    data = collect_data(user)

    user_tweet_list__sublist = user.tweet_list().sublist(test_start_date, test_end_date)
    real_process = ((user_tweet_list__sublist - test_start_date_unix) / 3600.).tolist()

    before = []
    for target in user.followers():
        before.append(time_being_in_top_k(real_process,
                                          data[target.user_id()]['target_wall_no_offset'],
                                          1, n,
                                          data[target.user_id()]['pi']))

    s_before = sum(before)

    if s_before == 0.:
        np.save('%sbad_%08d_%02d_visibility_avm' % (out_path_prefix, user.user_id(), month), [-1.])

    for test in ['avm', 'ravm']:
        test_competitor(test, user, data, month, s_before)


def test_competitor(test, user, data, month, s_before):
    print('testing %s' % test)

    best_intensity_ravm = np.tile(
        np.load('%s%08d_%02d_best_%s.npy' % (in_path_prefix, user.user_id(), month, test)), total_days)

    res = repeated_test(best_intensity_ravm, user, data) / s_before
    np.save('%s%08d_%02d_visibility_%s' % (out_path_prefix, user.user_id(), month, test), [res])

    print('done testing %s %d' % (test, user.user_id()))


def collect_data(user):
    data = {}

    for target in user.followers():
        print(target.user_id())
        test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(test_start_date, test_end_date)
        target_wall_no_offset = ((test_list._get_tweet_list() - test_start_date_unix) / 3600.).tolist()

        target_tweet_list__sublist = target.tweet_list().sublist(test_start_date, test_end_date)
        tweet_bags = target_tweet_list__sublist.get_periodic_intensity(n, test_start_date, test_end_date)

        pi = (tweet_bags > 0).astype(int)

        data[target.user_id()] = {
            'wall_no_offset': target_wall_no_offset,
            'pi': pi
        }

    return data


def main():
    multiprocessing.log_to_stderr(logging.INFO)
    good_users = list(set(np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()))
    rand_users = np.random.randint(0, len(good_users), 10)
    pool = multiprocessing.Pool(48)
    results = []
    for i in range(10):
        results.append(pool.apply_async(worker, (i + 1, good_users[rand_users[i]], 3,)))
    for i in range(10):
        results[i].get()


if __name__ == '__main__':
    main()
