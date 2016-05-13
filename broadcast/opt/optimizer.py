from __future__ import division, print_function
import numpy as np
from cvxopt import matrix, solvers
import pyximport; pyximport.install()

from ..opt import utils

# from opt import utils
# import opt_utils as utils
from ..data.user import User
import time
import sys


def get_projector_parameters(budget, upper_bounds):
    """ Generates parameters used in solver """
    n = len(upper_bounds)
    P = matrix(np.eye(n))
    G = np.zeros((2 * n, n))
    G[:n, :n] = np.diag([-1.] * n)
    G[n:, :n] = np.diag([1.] * n)
    G = matrix(G)
    h = np.zeros(2 * n)
    h[n:2 * n] = upper_bounds
    h = matrix(h)
    A = matrix(np.ones((1, n)))

    return [P, G, h, A, matrix([budget])]


def projection(q, P, G, h, A, C):
    """
    minimize    (1/2)*x'*x + q'*x
    subject to  G*x <= h
                A*x = b.
    """
    q = matrix(np.transpose(q))
    solvers.options['show_progress'] = False
    sol = solvers.qp(P, -q, G, h, A, C)

    return np.reshape(sol['x'], len(sol['x']))


def optimize_base(util, grad, proj, x0, threshold, gamma=0.8, c=0.5, verbose=False, with_iter=False):
    max_iterations = 50000
#     print('difference before and after proj:')
#     print(x0 - proj(x0))
#     print(np.linalg.norm(x0 - proj(x0)))
#     print('difference between utils')
#     print(util(x0) - util(proj(x0)))
#     print('util of the first point x0')
#     print(util(x0))

    x = proj(x0)

    for i in range(max_iterations):
        # print('iter %d' % i)
        # if i % 10 == 0:
        #     print(x)
        g = grad(x)

        d = proj(x + g * 1000.) - x
        s = gamma
        e_f = util(x)

        while util(x + s * d) - e_f < c * s * np.dot(np.transpose(g), d) and np.linalg.norm(s * d) > threshold:
            s *= gamma

        if np.linalg.norm(s * d) < threshold:
            break

        if util(x + s * d) - e_f > 0:
            x += s * d
        else:
            break

    if verbose:
        print('Done within %d iterations!' % i)

    if with_iter:
        return x, i
    else:
        return x


def optimize(util, util_grad, budget, upper_bounds, threshold, x0=None, verbose=False, with_iter=False):
    # start = int(round(time.time() * 1000))
    proj_params = get_projector_parameters(budget, upper_bounds)

    def proj(x):
        return projection(x, *proj_params)

    if sum(upper_bounds) <= budget:
        if verbose:
            print("obvious case")
        if with_iter:
            return upper_bounds, 0
        else:
            return upper_bounds

    opt_rates = optimize_base(util, util_grad, proj, x0, threshold, verbose=verbose, with_iter=with_iter)
    # delta = int(round(time.time() * 1000)) - start
    # sys.stderr.write('Total time: %d' % delta)
    return opt_rates


def learn_and_optimize(user, budget=None, upper_bounds=None,
                       period_length=24 * 7,
                       start_hour=0, end_hour=24,
                       learn_start_date=None, learn_end_date=None,
                       util=utils.weighted_top_one, util_gradient=utils.weighted_top_one_grad,
                       threshold=0.005,
                       extra_opt=None,
                       x0=None,
                       conn=None, inten=None):
    """
    :param budget: maximum budget we have
    :param period_length: length of the periods in hours
    :param util_gradient: gradient of the utility function
    :param util: utility function
    :param threshold: when norm of the difference of two consecutive iterations is less than this threshold, stop
    :param extra_opt: used for giving extra arguments to utility functions, such as k value
    :type user: User
    :type upper_bounds: np.ndarray
    :type start_hour: float
    :type end_hour: float
    :type learn_start_date: datetime
    :type learn_end_date: datetime
    :return: optimized intensity with respect to parameters given
    """

    if extra_opt is None:
        extra_opt = []

    user_tl = user.tweet_list().sublist(learn_start_date, learn_end_date)
    oi = user_tl.get_periodic_intensity(period_length, learn_start_date, learn_end_date)[start_hour:end_hour]

    if budget is None:
        budget = sum(oi)

    no_bad_users = 0
    if upper_bounds is None:
        upper_bounds, followers_wall_intensities = calculate_upper_bounds(user,
                                                                          learn_start_date, learn_end_date,
                                                                          start_hour, end_hour, oi, period_length)
    else:
        if inten is None:
            followers_wall_intensities = [
                np.array(target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(learn_start_date,
                                                                                learn_end_date).get_periodic_intensity(
                    period_length, learn_start_date, learn_end_date)[start_hour:end_hour])
                for target in user.followers()
                ]
        else:
            followers_wall_intensities = inten

    followers_weights = np.array([
        user.get_follower_weight(target)
        for target in user.followers()
        ])

    if conn is None:
        followers_conn_prob = [
            np.array(target.tweet_list().sublist(learn_start_date, learn_end_date).
                     get_connection_probability(period_length, learn_start_date, learn_end_date)[start_hour:end_hour])
            for target in user.followers()
            ]
    else:
        if type(conn) is dict:
            followers_conn_prob = [conn[target.user_id()] for target in user.followers()]
        else:
            followers_conn_prob = conn

    print('upper bounds: ')
    print(upper_bounds)
    print('budget: ')
    print(budget)

    def _util(x):
        return util(x, followers_wall_intensities, followers_conn_prob, followers_weights, *extra_opt)

    def _util_grad(x):
        return util_gradient(x, followers_wall_intensities, followers_conn_prob, followers_weights, *extra_opt)

    x0 = np.array([0.] * len(upper_bounds)) if x0 is None else x0
    return optimize(_util, _util_grad, budget, upper_bounds, threshold=threshold, x0=x0), upper_bounds


def calculate_upper_bounds(user, learn_start_date, learn_end_date, start_hour, end_hour, our_intensity, period_length):

    upper_bounds = np.zeros(len(our_intensity))
    followers_wall_intensities = []
    t_counter = 0.
    for target in user.followers():
        # Progressbar
        t_counter += 1
        print("\r[%% %.2f] processing %d" % (100. * t_counter / len(user.followers()), target.user_id()), end="")

        target_wall_t_list = target.wall_tweet_list(excluded_user_id=user.user_id())
        target_wall_t_list_sub = target_wall_t_list.sublist(learn_start_date, learn_end_date)
        target_wall_intensity_all = target_wall_t_list_sub.get_periodic_intensity(period_length, learn_start_date,
                                                                                  learn_end_date)
        target_wall_intensity = target_wall_intensity_all[start_hour:end_hour]

        followers_wall_intensities.append(np.array(target_wall_intensity))

        _max = max([0] + [our_intensity[i] / target_wall_intensity[i]
                          for i in range(len(our_intensity)) if target_wall_intensity[i] != 0.0])

        upper_bounds += user.get_follower_weight(target) * _max * np.array(target_wall_intensity)

    return upper_bounds, followers_wall_intensities
