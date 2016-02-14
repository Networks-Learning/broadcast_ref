from __future__ import division
import logging
import multiprocessing
import numpy as np
import sys
import traceback
from os.path import isfile

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
mid_path_prefix = '/local/moreka/new_np_data_mvm/'
out_path_prefix = '/local/moreka/new_np_results_mvm/'


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


def fetch_and_save_wall_and_conn(user, month):
    budget = len(user.tweet_list().sublist(test_start_date, test_end_date)) / total_days
    get_best_intensity_mvm(user, budget, month, 'test')

    budget = len(user.tweet_list().sublist(learn_start_date, learn_end_date)) / total_days
    get_best_intensity_mvm(user, budget, month, 'learn')


def get_best_intensity_mvm(user, budget, month, title):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))
    
    sum_conn_prob = np.sum(conn_probability_data, axis=1)
    indices = sum_conn_prob >= 0.1
    
    if not np.any(indices):
        raise RuntimeError('This is really a bad user: %d since no good followers!' % user.user_id())
    
    if title == 'learn':
        user_tweet_list__sublist = user.tweet_list().sublist(learn_start_date, learn_end_date)
        user_learned_intensity = np.array(user_tweet_list__sublist.get_periodic_intensity(24, learn_start_date, learn_end_date))
    else:
        user_tweet_list__sublist = user.tweet_list().sublist(test_start_date, test_end_date)
        user_learned_intensity = np.array(user_tweet_list__sublist.get_periodic_intensity(24, test_start_date, test_end_date))
    
    best_intensity,_ = learn_and_optimize(user, budget=budget,
                                          learn_start_date=learn_start_date,
                                          learn_end_date=learn_end_date,
                                          util=max_min_top_one_smt,
                                          util_gradient=max_min_top_one_smt_grad,
                                          start_hour=0, end_hour=24,
                                          period_length=24,
                                          upper_bounds=np.ones(24) * 1000.,
                                          threshold=0.001,
                                          conn=conn_probability_data[indices],
                                          inten=wall_intensity_data[indices],
                                          x0=user_learned_intensity)

    np.save('%s%08d_%02d_best_mvm_%s' % (mid_path_prefix, user.user_id(), month, title), np.array(best_intensity))


def do_theoretical_test(user, month):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))

    sum_conn_prob = np.sum(conn_probability_data, axis=1)
    indices = sum_conn_prob >= 0.1
    
    wall_intensity_data = wall_intensity_data[indices]
    conn_probability_data = conn_probability_data[indices]
    
    user_tweet_list__sublist = user.tweet_list().sublist(learn_start_date, learn_end_date)
    user_learned_intensity = np.array(user_tweet_list__sublist.get_periodic_intensity(24, learn_start_date, learn_end_date))

    weights = np.ones(1)  # just a dummy value ;-)
    
    before = max_min_top_one_smt(user_learned_intensity, wall_intensity_data, conn_probability_data, weights)
    if before == 0.:
        np.save('%sbad_%08d_%02d_mvm_vis_theo' % (out_path_prefix, user.user_id(), month), [-1.])
        return

    for test in ['mvm']:
        best_intensity = np.load('%s%08d_%02d_best_%s_learn.npy' % (mid_path_prefix, user.user_id(), month, test))
        theo_result = max_min_top_one_smt(best_intensity, wall_intensity_data, conn_probability_data, weights)

        np.save('%s%08d_%02d_vis_theo_mvm_%s' % (out_path_prefix, user.user_id(), month, test), [theo_result / before, theo_result, before])

    for test in ['iavm', 'pavm', 'ravm']:
        best_intensity = np.load('%s%08d_%02d_best_%s_learn.npy' % (in_path_prefix, user.user_id(), month, test))
        theo_result = max_min_top_one_smt(best_intensity, wall_intensity_data, conn_probability_data, weights)

        np.save('%s%08d_%02d_vis_theo_mvm_%s' % (out_path_prefix, user.user_id(), month, test), [theo_result / before, theo_result, before])


def do_practical_test(user, month):
    data = collect_data(user)

    user_tweet_list__sublist = user.tweet_list().sublist(test_start_date, test_end_date)._get_tweet_list()
    real_process = ((user_tweet_list__sublist - test_start_date_unix) / 3600.).tolist()

    before = []
    for target in user.followers():
        before.append(time_being_in_top_k(real_process,
                                          data[target.user_id()]['wall_no_offset'], 1, n,
                                          data[target.user_id()]['pi']))
    
    before = np.array(before)
    before_p = before[before > 0]

    if before_p.shape[0] == 0:
        np.save('%sbad_%08d_%02d_vis_real_mvm' % (out_path_prefix, user.user_id(), month), [-1.])
        return
    
    before.sort()
    s_before = np.mean(before[:10])
    
    if s_before == 0.:
        return

    for test in ['mvm', 'ravm', 'pavm', 'iavm']:
        test_competitor(test, user, data, month, s_before)


def test_competitor(test, user, data, month, s_before):
    print('testing %s' % test)

    best_intensity = np.tile(
        np.load('%s%08d_%02d_best_%s_test.npy' % (in_path_prefix, user.user_id(), month, test)), total_days)

    res = repeated_test(best_intensity, user, data) / s_before
    if res >= 0.:
        np.save('%s%08d_%02d_vis_real_mvm_%s' % (out_path_prefix, user.user_id(), month, test), [res])

    print('done testing %s %d' % (test, user.user_id()))


def repeated_test(intensity, user, data):
    result = []
    for iteration in range(10):
        simulated_process = generate_piecewise_constant_poisson_process(intensity)
        now = []
        for target in user.followers():
            now.append(time_being_in_top_k(simulated_process,
                                           data[target.user_id()]['wall_no_offset'], 1, n,
                                           data[target.user_id()]['pi']))
        if len(now):
            now.sort()
            result.append(np.mean(now[:10]))

    if len(result):
        return np.mean(result)
    else:
        return -1


def collect_data(user):
    data = {}
    
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), 3))
    sum_conn_prob = np.sum(conn_probability_data, axis=1)
    indices = sum_conn_prob >= 0.1
    
    new_followers = []
    
    for i in range(len(user.followers())):
        if indices[i]:
            new_followers.append(user.followers()[i])
    
    user._followers__ = new_followers

    for target in user.followers():
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


def do_simulation_test(user, month):
    wall_intensity_data = np.load('%s%08d_%02d_wall.npy' % (in_path_prefix, user.user_id(), month))
    conn_probability_data = np.load('%s%08d_%02d_conn.npy' % (in_path_prefix, user.user_id(), month))

    user_tweet_list__sublist = user.tweet_list().sublist(learn_start_date, learn_end_date)
    user_learned_intensity = np.array(user_tweet_list__sublist.get_periodic_intensity(24, learn_start_date, learn_end_date))
    
    sum_conn_prob = np.sum(conn_probability_data, axis=1)
    indices = sum_conn_prob >= 0.1
    
    new_followers = []
    for i in range(len(user.followers())):
        if indices[i]:
            new_followers.append(user.followers()[i])
    
    user._followers__ = new_followers
    
    conn_probability_data = conn_probability_data[indices]
    wall_intensity_data = wall_intensity_data[indices]
    
    for test in ['mvm', 'ravm', 'iavm', 'pavm']:
        if test == 'mvm':
            path = mid_path_prefix
        else:
            path = in_path_prefix

        try:
            user_best_intensity = np.load(
                '%s%08d_%02d_best_%s_learn.npy' % (path, user.user_id(), month, test))
        except IOError:
            continue
            
        results = []
        
        if sum(user_best_intensity) > 1e-6 and sum(user_learned_intensity) > 1e-6:
            now, before = [], []

            for i in range(300):
                user_best_realization = generate_piecewise_constant_poisson_process(user_best_intensity)
                user_learned_realization = generate_piecewise_constant_poisson_process(user_learned_intensity)

                vis_now = []
                vis_before = []
                for idx, target in enumerate(user.followers()):
                    target_learned_intensity = wall_intensity_data[idx]
                    target_learned_connection = conn_probability_data[idx]
                    target_realization = generate_piecewise_constant_poisson_process(target_learned_intensity)

                    vis_before.append(time_being_in_top_k(user_learned_realization,
                                                      target_realization, 1, 24,
                                                      target_learned_connection))

                    vis_now.append(time_being_in_top_k(user_best_realization,
                                                   target_realization, 1, 24,
                                                   target_learned_connection))
                
                vis_now.sort()
                vis_before.sort()
                
                now.append(np.sum(vis_now[:10]))
                before.append(np.sum(vis_before[:10]))

            if sum(before) < 1e-6 or sum(now) < 1e-6:
                print('is it really possible!!! for user %d' %user.user_id())
                break
            else:
                results.append(sum(now)/sum(before))


        if len(results) == 0.:
            np.save('%sbad_%08d_%02d_vis_simul' % (out_path_prefix, user.user_id(), month), [-1.])
            return
        
#         print('the result for the user %d is %.2f' %(user.user_id(), np.mean(results)))
        np.save('%s%08d_%02d_vis_simul_%s' % (out_path_prefix, user.user_id(), month, test), [np.mean(results)])


def main():
    commands = {
        'fetch': fetch_and_save_wall_and_conn,
        'prac': do_practical_test,
        'theo': do_theoretical_test,
        'simul': do_simulation_test,
#         'comp': do_calculate_competitors,
    }

    funcs = []
    for arg in sys.argv:
        if arg in commands:
            funcs.append(commands[arg])

    multiprocessing.log_to_stderr(logging.INFO)

    good_users = np.load('good_users.npy').tolist()[100:]

    pool = multiprocessing.Pool(48)
    results = []
    for i in range(len(good_users)):
        results.append(pool.apply_async(worker, (i + 1, int(good_users[i]), 3, funcs, )))
    for i in range(len(good_users)):
        results[i].get()


if __name__ == '__main__':
    main()
