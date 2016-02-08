from __future__ import division
import logging
import multiprocessing
import numpy as np
import sys
import traceback

sys.path.append('/local/moreka/broadcast-ref')

from datetime import datetime, timedelta
from data.db_connector import DbConnection
from data.hdfs import HDFSLoader
from data.user import User
from data.user_repo import HDFSSQLiteUserRepository
from opt.optimizer import learn_and_optimize
from opt.utils import *
from util.cal import unix_timestamp
from simulator.simulate import generate_piecewise_constant_poisson_process, time_being_in_top_k
from competitors.avm import ravm, ipavm

test_start_date = datetime(2009, 5, 14)
test_end_date = datetime(2009, 8, 13)
test_start_date_unix = unix_timestamp(test_start_date)

total_days = int((test_end_date - test_start_date).days)
n = total_days * 24

learn_start_date = test_start_date - timedelta(days=3 * 30)  # TODO: suppose month is 3
learn_end_date = test_start_date - timedelta(seconds=1)

in_path_prefix = '/local/moreka/new_np_data/'
out_path_prefix = '/local/moreka/new_np_results/'


def fetch_wall_array(user):
    intensity_arr = np.zeros((len(user.followers()), 24))
    connection_arr = np.zeros((len(user.followers()), 24))

    for i in range(len(user.followers())):
        target = user.followers()[i]
        print(target.user_id())
        sublist = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(learn_start_date, learn_end_date)
        
        intensity_arr[i, :] = sublist.get_periodic_intensity(24, learn_start_date, learn_end_date)
        
        target_sublist = target.tweet_list().sublist(learn_start_date, learn_end_date)
        connection_arr[i, :] = target_sublist.get_connection_probability(24, learn_start_date, learn_end_date)

    return intensity_arr, connection_arr


def worker(pid, user_id, month, funcs):
    print '[Process-%d] Worker started for user %d' % (pid, user_id)

    repo = HDFSSQLiteUserRepository(HDFSLoader(), DbConnection())
    user = User(user_id, repo)

    try:
        for func in funcs:
            func(user, month)
    except:
        sys.stderr.write('ERROR FOR USER %d, SO DROPPED. REASON:' % user_id)
        traceback.print_exc()
        


def do_theoretical_test(user, month):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))

    user_tweet_list__sublist = user.tweet_list().sublist(learn_start_date, learn_end_date)
    user_learned_intensity = np.array(user_tweet_list__sublist.get_periodic_intensity(24, learn_start_date, learn_end_date))

    target_count = wall_intensity_data.shape[0]
    weights = np.ones(target_count) * (1. / target_count)

    before = weighted_top_one(user_learned_intensity, wall_intensity_data, conn_probability_data, weights)
    if before == 0.:
        np.save('%sbad_%08d_%02d_vis_theo' % (out_path_prefix, user.user_id(), month), [-1.])
        return

    for test in ['avm', 'ravm', 'iavm', 'pavm']:
        best_intensity = np.load('%s%08d_%02d_best_%s_learn.npy' % (in_path_prefix, user.user_id(), month, test))
        theo_result = weighted_top_one(best_intensity, wall_intensity_data, conn_probability_data, weights)

        np.save('%s%08d_%02d_vis_theo_%s' % (out_path_prefix, user.user_id(), month, test), [theo_result / before])

        
def get_best_intensity(user, budget, month, title):
    best_intensity, upper_bounds = learn_and_optimize(user, budget=budget,
                                                      learn_start_date=learn_start_date,
                                                      learn_end_date=learn_end_date,
                                                      util=weighted_top_one,
                                                      util_gradient=weighted_top_one_grad,
                                                      start_hour=0, end_hour=24,
                                                      period_length=24,
                                                      upper_bounds=np.ones(24) * 1000.,
                                                      threshold=0.02)

    np.save('%s%08d_%02d_best_avm_%s' % (in_path_prefix, user.user_id(), month, title), np.array(best_intensity))

    best_ravm = ravm(budget, upper_bounds)
    np.save('%s%08d_%02d_best_ravm_%s' % (in_path_prefix, user.user_id(), month, title), best_ravm)


def fetch_and_save_wall_and_conn(user, month):
    wall_intensity_data, conn_probability_data = fetch_wall_array(user)

    np.save('%s%08d_%02d_wall' % (in_path_prefix, user.user_id(), month), wall_intensity_data)
    np.save('%s%08d_%02d_conn' % (in_path_prefix, user.user_id(), month), conn_probability_data)

    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_days
    get_best_intensity(user, budget, month, 'test')

    budget = len(user.tweet_list().sublist(learn_start_date, learn_end_date)) / total_days
    get_best_intensity(user, budget, month, 'learn')


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

    user_tweet_list__sublist = user.tweet_list().sublist(test_start_date, test_end_date)._get_tweet_list()
    real_process = ((user_tweet_list__sublist - test_start_date_unix) / 3600.).tolist()

    before = []
    for target in user.followers():
        before.append(time_being_in_top_k(real_process,
                                          data[target.user_id()]['wall_no_offset'], 1, n,
                                          data[target.user_id()]['pi']))

    s_before = sum(before)

    if s_before == 0.:
        np.save('%sbad_%08d_%02d_vis_real' % (out_path_prefix, user.user_id(), month), [-1.])

    for test in ['avm', 'ravm', 'pavm', 'iavm']:
        test_competitor(test, user, data, month, s_before)


def do_simulation_test(user, month):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.tile(
        np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month)), total_days)

    user_tweet_list__sublist = user.tweet_list().sublist(learn_start_date, learn_end_date)
    user_learned_intensity = np.tile(
        np.array(user_tweet_list__sublist.get_periodic_intensity(24, learn_start_date, learn_end_date)), total_days)
    
    for test in ['avm', 'ravm', 'iavm', 'pavm']:
        print('testing simul %s for %d' % (test, user.user_id()))
        
        try:
            user_best_intensity = np.tile(
                np.load('%s%08d_%02d_best_%s_learn.npy' % (in_path_prefix, user.user_id(), month, test)), total_days)
        except IOError:
            continue
        
        results = []
        for i in range(10):
            user_best_realization = generate_piecewise_constant_poisson_process(user_best_intensity)
            user_learned_realization = generate_piecewise_constant_poisson_process(user_learned_intensity)
            
            now, before = [], []
            for idx, target in enumerate(user.followers()):
                target_learned_intensity = wall_intensity_data[idx]
                target_learned_connection = conn_probability_data[idx]
                target_realization = generate_piecewise_constant_poisson_process(target_learned_intensity)

                before.append(time_being_in_top_k(user_learned_realization,
                                                  target_realization, 1, n,
                                                  target_learned_connection))

                now.append(time_being_in_top_k(user_best_realization,
                                               target_realization, 1, n,
                                               target_learned_connection))
            if (sum(before) == 0.):
                continue

            results.append(sum(now) / sum(before))

        if len(results) == 0.:
            np.save('%sbad_%08d_%02d_vis_simul' % (out_path_prefix, user.user_id(), month), [-1.])
            return
        
        np.save('%s%08d_%02d_vis_simul_%s' % (out_path_prefix, user.user_id(), month, test), [np.mean(results)])
        

def test_competitor(test, user, data, month, s_before):
    print('testing %s' % test)

    best_intensity = np.tile(
        np.load('%s%08d_%02d_best_%s_test.npy' % (in_path_prefix, user.user_id(), month, test)), total_days)

    res = repeated_test(best_intensity, user, data) / s_before
    np.save('%s%08d_%02d_vis_real_%s' % (out_path_prefix, user.user_id(), month, test), [res])

    print('done testing %s %d' % (test, user.user_id()))


def do_calculate_competitors(user, month):
    print('... user %d ...' % user.user_id())
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))
    
    upper_bounds = np.ones(24) * 1000.
    
    title = 'learn'
    budget = len(user.tweet_list().sublist(learn_start_date, learn_end_date)) / total_days
    iavm_best_intensity = ipavm(budget, upper_bounds, wall_intensity_data)
    pavm_best_intensity = ipavm(budget, upper_bounds, wall_intensity_data, conn_probability_data)
    
    np.save('%s%08d_%02d_best_iavm_%s' % (in_path_prefix, user.user_id(), month, title), np.array(iavm_best_intensity))
    np.save('%s%08d_%02d_best_pavm_%s' % (in_path_prefix, user.user_id(), month, title), np.array(pavm_best_intensity))
    
    title = 'test'
    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_days
    iavm_best_intensity = ipavm(budget, upper_bounds, wall_intensity_data)
    pavm_best_intensity = ipavm(budget, upper_bounds, wall_intensity_data, conn_probability_data)
    
    np.save('%s%08d_%02d_best_iavm_%s' % (in_path_prefix, user.user_id(), month, title), np.array(iavm_best_intensity))
    np.save('%s%08d_%02d_best_pavm_%s' % (in_path_prefix, user.user_id(), month, title), np.array(pavm_best_intensity))


def collect_data(user):
    data = {}

    for target in user.followers():
        print(target.user_id())
        test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(test_start_date, test_end_date)
        target_wall_no_offset = ((test_list._get_tweet_list() - test_start_date_unix) / 3600.).tolist()

        target_tweet_list__sublist = target.tweet_list().sublist(test_start_date, test_end_date)
        tweet_bags = np.array(target_tweet_list__sublist.get_periodic_intensity(n, test_start_date, test_end_date))

        pi = (tweet_bags > 0).astype(int)

        data[target.user_id()] = {
            'wall_no_offset': target_wall_no_offset,
            'pi': pi
        }

    return data


def main():
    commands = {
        'fetch': fetch_and_save_wall_and_conn,
        'prac': do_practical_test,
        'theo': do_theoretical_test,
        'simul': do_simulation_test,
        'comp': do_calculate_competitors,
    }

    funcs = []
    for arg in sys.argv:
        if arg in commands:
            funcs.append(commands[arg])

    multiprocessing.log_to_stderr(logging.INFO)

#     good_users = list(set(np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()))
    good_users = np.load('good_users.npy').tolist()
#     good_users = [803059]

    pool = multiprocessing.Pool(48)
    results = []
    for i in range(len(good_users)):
        results.append(pool.apply_async(worker, (i + 1, int(good_users[i]), 3, funcs, )))
    for i in range(len(good_users)):
        results[i].get()


if __name__ == '__main__':
    main()
