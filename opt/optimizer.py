from __future__ import division
import numpy as np
from cvxopt import matrix, solvers
from data.models import Intensity
from opt import utils
from data.user import User


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
    A = matrix(np.ones((1., n)))

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


def optimize_base(util, grad, proj, x0, threshold, gamma=0.9, c=1.):
    max_iterations = 1000
    x = proj(x0)

    for i in range(max_iterations):
        if i % 10 == 0:
            print('iter %d' % i)
            print(x)
        g = grad(x)
        d = proj(x + g) - x
        s = gamma
        e_f = util(x)
        while util(x + s * d) - e_f > c * gamma * np.dot(np.transpose(g), d) and \
                        np.linalg.norm(s * d) >= threshold:
            s *= gamma

        x += s * d
        if np.linalg.norm(s * d) < threshold:
            break

    return x


def optimize(util, util_grad, budget, upper_bounds, threshold, x0=None):
    proj_params = get_projector_parameters(budget, upper_bounds)

    def proj(x):
        return projection(x, *proj_params)

    if sum(upper_bounds) <= budget:
        return None

    x0 = [0.] * len(upper_bounds) if x0 is None else x0

    opt_rates = optimize_base(util, util_grad, proj, x0, threshold)
    return opt_rates


def learn_and_optimize(user, budget=None, upper_bounds=None,
                       period_length=24 * 7, time_slots=None,
                       start_hour=0, end_hour=24,
                       learn_start_date=None, learn_end_date=None,
                       util=utils.weighted_top_one, util_gradient=utils.weighted_top_one_grad,
                       threshold=0.005):
    """
    :type user: User
    :type upper_bounds: np.ndarray
    :type start_hour: float
    :type end_hour: float
    :type learn_start_date: datetime
    :type learn_end_date: datetime
    :return: optimized intensity with respect to parameters given
    """

    if time_slots is None:
        time_slots = [1.] * period_length

    user_tl = user.tweet_list().sublist(learn_start_date, learn_end_date)
    oi = user_tl.get_periodic_intensity(period_length, time_slots).sub_intensity(start_hour, end_hour)

    if budget is None:
        budget = sum([x['rate'] * x['length'] for x in oi])
    
    no_bad_users = 0
    if upper_bounds is None:
        upper_bounds = np.zeros(oi.size())
        followers_wall_intensities = []

        for target in user.followers():
            target_wall_intensity = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(learn_start_date, learn_end_date)\
                .get_periodic_intensity(period_length, time_slots) \
                .sub_intensity(start_hour, end_hour)

            followers_wall_intensities.append(target_wall_intensity)

            _max = max([0] + [oi[i]['rate'] / target_wall_intensity[i]['rate']
                        for i in range(oi.size()) if target_wall_intensity[i]['rate'] != 0.0])
            
            if _max == 0:
                no_bad_users += 1

            upper_bounds += user.get_follower_weight(target) * _max * \
                            np.array(target_wall_intensity.get_as_vector()[0])
    else:
        followers_wall_intensities = [
            target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(learn_start_date, learn_end_date) \
                .get_periodic_intensity(period_length, time_slots) \
                .sub_intensity(start_hour, end_hour)
            for target in user.followers()
        ]

    followers_weights = [
        user.get_follower_weight(target)
        for target in user.followers()
        ]

    followers_conn_prob = [
        target.tweet_list().sublist(learn_start_date, learn_end_date).get_connection_probability()[start_hour:end_hour]
        for target in user.followers()
        ]
    
    print 'upper bounds: '
    print upper_bounds
    print 'budget: '
    print budget
    print 'bad users: '
    print no_bad_users

    def _util(x):
        return util(Intensity(x), followers_wall_intensities, followers_conn_prob, followers_weights)

    def _util_grad(x):
        return util_gradient(Intensity(x), followers_wall_intensities, followers_conn_prob, followers_weights)

    return optimize(_util, _util_grad, budget, upper_bounds, threshold=threshold)
