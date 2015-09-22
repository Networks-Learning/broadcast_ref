from __future__ import division
from math import factorial
import numpy as np
from scipy.integrate import quad, trapz
from cvxopt import matrix, solvers
from data.models import Intensity


def f(t, k, b, c, h):
    """
    :param t: time of evaluation of f
    :param k: top-k
    :param b: lambda2 value during time interval
    :param c: lambda1 value during time interval
    :param h: initial values at t=0
    :return: f(t) if b + c is not zero, otherwise returns h
    """
    if b + c < 1e-10:
        print 'here'
        return h

    alpha = np.zeros((k, k))
    p = c / (b + c)

    poly_t = np.array([(t * b) ** x / factorial(x) for x in range(k)])
    betas = np.array([1 - (1 - p) ** x for x in range(1, k + 1)])

    for i in range(k):
        for j in range(i + 1):
            alpha[i, j] = h[i - j] - betas[i - j]

    return np.dot(alpha, poly_t) * np.exp(-(b + c) * t) + betas


def f_single_valued(t, k, b, c, h):
    return f(t, k, b, c, h)[k-1]


def expected_f(lambda1, lambda2, k, h0=None, pi=None):
    """
    :param lambda1: need not to have valid time slot lengths
    :param lambda2: must have valid time slot lengths
    :type lambda1: Intensity
    :type lambda2: Intensity
    :param k: top-k
    :param h0: initial value array at t=0
    :param pi: probability of being online in each interval
    :return: expected time being on top-k
    """
    return expected_f_trapz(lambda1, lambda2, k, h0, pi)


def expected_f_trapz(lambda1, lambda2, k, h0=None, pi=None):
    """
    :param lambda1: need not to have valid time slot lengths
    :param lambda2: must have valid time slot lengths
    :type lambda1: Intensity
    :type lambda2: Intensity
    :param k: top-k
    :param h0: initial value array at t=0
    :param pi: probability of being online in each interval
    :return: expected time being on top-k
    """
    h0 = [0.] * k if h0 is None else h0

    e_f = 0
    sample_count = 11
    for i in range(lambda2.size()):
        samples = np.linspace(0, lambda2[i]['length'], 10)
        values = [f_single_valued(sample, k, lambda2[i]['rate'], lambda1[i]['rate'], h0) for sample in samples]
        e_f += pi[i] * trapz(values, samples)
        
        h0 = f(lambda2[i]['length'], k, lambda2[i]['rate'], lambda1[i]['rate'], h0)
    return e_f


def expected_f_quad(lambda1, lambda2, k, h0=None, pi=None):
    """
    :param lambda1: need not to have valid time slot lengths
    :param lambda2: must have valid time slot lengths
    :type lambda1: Intensity
    :type lambda2: Intensity
    :param k: top-k
    :param h0: initial value array at t=0
    :param pi: probability of being online in each interval
    :return: expected time being on top-k
    """
    h0 = [0.] * k if h0 is None else h0

    e_f = 0
    for i in range(lambda2.size()):
        e_f += pi[i] * quad(f_single_valued, 
                            0, lambda2[i]['length'], 
                            (k, lambda2[i]['rate'], lambda1[i]['rate'], h0),
                            epsabs=1e-3
                           )[0]
        h0 = f(lambda2[i]['length'], k, lambda2[i]['rate'], lambda1[i]['rate'], h0)
    return e_f


def gradient(lambda1, lambda2, k, h0=None, pi=None):
    """
    :param lambda1: need not to have valid time slot lengths
    :param lambda2: must have valid time slot lengths
    :type lambda1: Intensity
    :type lambda2: Intensity
    :param k: top-k
    :param h0: initial value array at t=0
    :return: gradient of function
    """

    n = lambda2.size()
    g = np.array([0.] * n)
    epsilon = 0.0001

    for i in range(n):
        if lambda1[i]['rate'] >= epsilon:
            lambda1[i]['rate'] -= epsilon
            f1 = expected_f(lambda1, lambda2, k, h0, pi=pi)
            lambda1[i]['rate'] += 2. * epsilon
            f2 = expected_f(lambda1, lambda2, k, h0, pi=pi)
            g[i] = (f2 - f1) / (2. * epsilon)
            lambda1[i]['rate'] -= epsilon
        else:
            f1 = expected_f(lambda1, lambda2, k, h0, pi=pi)
            lambda1[i]['rate'] += epsilon
            f2 = expected_f(lambda1, lambda2, k, h0, pi=pi)
            g[i] = (f2 - f1) / epsilon
            lambda1[i]['rate'] -= epsilon
    return g


def get_projector_parameters(budget, upper_bounds):
    """ Generates parameters used in solver """
    n = len(upper_bounds)
    P = matrix(np.eye(n))
    G = np.zeros((2 * n, n))
    G[:n, :n] = np.diag([-1.] * n)
    G[n:, :n] = np.diag([1.] * n)
    G = matrix(G)
    h = matrix(np.array([0.] * n + upper_bounds))
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
    x = x0

    for i in range(max_iterations):
        if i % 10 == 0:
            print('iter %d' % i)
            print(x)
        g = grad(x)
        d = proj(x + g) - x
        s = gamma
        e_f = util(x)
        while util(x + s * d) - e_f > c * gamma * np.dot(np.transpose(g), d) and \
                np.linalg.norm(s * d) >= threshold ** 2:
            s *= gamma

        x += s * d
        if np.linalg.norm(s * d) < threshold ** 2:
            break

    return x


def optimize(lambda2, k, budget, upper_bounds, threshold=0.001, x0=None, pi=None):
    """
    :param lambda2: other's intensities
    :type lambda2: Intensity
    :param k: top-k
    :param x0: start point
    """
    if pi is None:
        pi = [1.] * lambda2.size()
    else:
        s = sum(pi)
        pi = [p * 24. / s for p in pi]
    
    def _f(x):
        return expected_f(Intensity(x), lambda2, k, pi=pi)

    def grad(x):
        return gradient(Intensity(x), lambda2, k, pi=pi)

    proj_params = get_projector_parameters(budget, upper_bounds)

    def proj(x):
        return projection(x, *proj_params)

    x0 = [0.] * lambda2.size() if x0 is None else x0

    if sum(upper_bounds) <= budget:
        opt_rates = Intensity()
        for i in range(lambda2.size()):
            opt_rates.append(upper_bounds[i], lambda2[i]['length'])
        return opt_rates
    
    opt_rates = optimize_base(_f, grad, proj, x0, threshold)
    return Intensity(opt_rates).copy_lengths(lambda2)
