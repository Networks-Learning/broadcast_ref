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

test_start_date = datetime(2009, 5, 14)
test_end_date = datetime(2009, 8, 13)

total_days = int((test_end_date - test_start_date).days)

in_path_prefix = '/local/moreka/np_data/'
out_path_prefix = '/local/moreka/np_results/'

THEORETICAL = 1
SIMULATION = 2
PRACTICAL = 3


def do_theoretical_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn):
    pass


def do_simulation_work(wall_intensity_data, conn_probability_data, best_intensity, num_months_to_learn):
    pass


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

#     good_users = list(set(np.loadtxt('/local/moreka/broadcast-ref/Good-Users.txt', dtype='int').tolist()))
    good_users = [16173435, 33830602, 16648152, 17404514, 6094672, 21010474]
#     good_users = [33830602]

    jobs = []
    for i in range(len(good_users)):
        for work in [SIMULATION, THEORETICAL]:
            p = multiprocessing.Process(target=worker, args=(i + 1, good_users[i], 3, work))
            jobs.append(p)
            p.start()

    for j in jobs:
        j.join()
        sys.stderr.write('%s.exitcode = %s\n' % (j.name, j.exitcode))
