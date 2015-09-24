from __future__ import division
import numpy as np
from cvxopt import matrix, solvers


def get_projector_parameters(budget, upper_bounds):
    """ Generates parameters used in solver """
    n = len(upper_bounds)
    P = matrix(np.eye(n))
    G = np.zeros((2 * n, n))
    G[:n, :n] = np.diag([-1.] * n)
    G[n:, :n] = np.diag([1.] * n)
    G = matrix(G)
    h = np.zeros(2 * n)
    h[n:2*n] = upper_bounds
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
#     solvers.options['show_progress'] = True
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