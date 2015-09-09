from math import factorial
import numpy as np
from scipy.integrate import quad
from cvxopt import matrix, solvers


def f(t, k, b, c, h):
    _f = np.zeros((k, k))
    p = c / (b + c)

    poly_t = np.array([(t * b) ** x / factorial(x) for x in range(k)])
    betas = np.array([1 - (1 - p) ** x for x in range(1, k + 1)])

    for i in range(k):
        for j in range(i + 1):
            _f[i, j] = h[i - j] - betas[i - j]

    return np.dot(_f, poly_t) * np.exp(-(b + c) * t) + betas


def expected_f(lambda1, lambda2, k, h0=None):
    h0 = [0.] * k if h0 is None else h0

    e_f = 0
    t = 0
    for i in range(len(lambda2.time_intervals)):
        e_f += quad(f, 0, lambda2.time_intervals[i], (k, lambda2.rates[i], lambda1.rates[i], h0))[0]
        h0 = f(lambda2.time_intervals[i], k, lambda2.rates[i], lambda1.rates[i], h0)
        t += lambda2.time_intervals[i]
    return e_f


def gradient(lambda1, lambda2, k, h0=None):
    n = len(lambda2.time_intervals)
    g = np.array([0.] * n)
    epsilon = 0.0001

    for i in range(n):
        if lambda1.rates[i] >= epsilon:
            lambda1.rates[i] -= epsilon
            f1 = expected_f(lambda1, lambda2, k, h0)
            lambda1.rates[i] += 2. * epsilon
            f2 = expected_f(lambda1, lambda2, k, h0)
            g[i] = (f2 - f1) / (2. * epsilon)
            lambda1.rates[i] -= epsilon
        else:
            f1 = expected_f(lambda1, lambda2, k, h0)
            lambda1.rates[i] += epsilon
            f2 = expected_f(lambda1, lambda2, k, h0)
            g[i] = (f2 - f1) / epsilon
            lambda1.rates[i] -= epsilon
    return g


def get_projector_parameters(C, upper_bounds):
    """ Generates parameters used in solver """
    n = len(upper_bounds)
    P = matrix(np.eye(n))
    G = np.zeros((2 * n, n))
    G[:n, :n] = np.diag([-1.] * n)
    G[n:, :n] = np.diag([1.] * n)
    G = matrix(G)
    h = matrix(np.array([0.] * n + upper_bounds))
    A = matrix(np.ones((1., n)))

    return [P, G, h, A, matrix([C])]


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


def optimize(util, grad, proj, x0, gamma=0.8, c=1.):
    max_iterations = 100
    threshold = 0.0001

    x = x0

    for i in range(max_iterations):
        print('iter %d' % i)
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
