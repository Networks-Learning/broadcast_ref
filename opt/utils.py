from math import factorial, exp
import numpy as np
from scipy.integrate import quad, trapz


def f(t, k, b, c, h):
    """
    :param t: time of evaluation of f
    :param k: top-k
    :param b: lambda2 value during time interval
    :param c: lambda1 value during time interval
    :param h: initial values at t=0
    :type h: numpy.ndarray
    :return: f(t) if b + c is not zero, otherwise returns h
    """
    if b + c < 1e-10:
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
    return f(t, k, b, c, h)[k - 1]


def expected_f_top_k(lambda1, lambda2, k, h0=None, pi=None):
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

    h0 = [0.] * k if h0 is None else h0

    e_f = 0
    sample_count = 11
    for i in range(lambda2.size()):
        samples = np.linspace(0, lambda2[i]['length'], sample_count)
        values = [f_single_valued(sample, k, lambda2[i]['rate'], lambda1[i]['rate'], h0) for sample in samples]
        e_f += pi[i] * trapz(values, samples)

        h0 = f(lambda2[i]['length'], k, lambda2[i]['rate'], lambda1[i]['rate'], h0)

    return e_f


def expected_f_quad(lambda1, lambda2, k, h0=None, pi=None):

    h0 = [0.] * k if h0 is None else h0

    e_f = 0
    for i in range(lambda2.size()):
        e_f += pi[i] * quad(f_single_valued,
                            0, lambda2[i]['length'],
                            (k, lambda2[i]['rate'], lambda1[i]['rate'], h0),
                            epsabs=1e-3)[0]

        h0 = f(lambda2[i]['length'], k, lambda2[i]['rate'], lambda1[i]['rate'], h0)
    return e_f


def gradient_top_k(lambda1, lambda2, k, h0=None, pi=None):
    """
    :param lambda1: need not to have valid time slot lengths
    :param lambda2: must have valid time slot lengths
    :type lambda1: Intensity
    :type lambda2: Intensity
    :param k: top-k
    :param h0: initial value array at t=0
    :return: gradient of function
    """
    
    h0 = [0.] * k if h0 is None else h0

    n = lambda2.size()
    grad = np.array([0.] * n)
    epsilon = 0.0001

    for i in range(n):
        if lambda1[i]['rate'] >= epsilon:
            lambda1[i]['rate'] -= epsilon
            f1 = expected_f_top_k(lambda1, lambda2, k, h0, pi=pi)
            lambda1[i]['rate'] += 2. * epsilon
            f2 = expected_f_top_k(lambda1, lambda2, k, h0, pi=pi)
            grad[i] = (f2 - f1) / (2. * epsilon)
            lambda1[i]['rate'] -= epsilon
        else:
            f1 = expected_f_top_k(lambda1, lambda2, k, h0, pi=pi)
            lambda1[i]['rate'] += epsilon
            f2 = expected_f_top_k(lambda1, lambda2, k, h0, pi=pi)
            grad[i] = (f2 - f1) / epsilon
            lambda1[i]['rate'] -= epsilon
    return grad


def f_top_one(t, b, c, h):
    return f(t, 1, b, c, [h])[0]


def expected_f_top_one(lambda1, lambda2, pi):
    M = lambda1.size()
    e_f = 0
    h = 0
    for m in range(M):
        bm = lambda2[m]['rate']
        cm = lambda1[m]['rate']
        dt = lambda2[m]['length']

        if bm + cm < 1e-10:
            e_f += h * dt
        else:
            p = cm / (bm + cm)
            e_f += pi[m] * ((h - p) * (1. - exp(-dt * (bm + cm))) / (bm + cm) + p * dt)
            h = f_top_one(dt, bm, cm, h)

    return e_f


def h_va_shoraka(lambda1, lambda2):
    M = lambda1.size()
    h = np.zeros(M+1)
    dh_dc = np.zeros((M, M))
    q = np.zeros(M)

    h[-1] = 0
    for m in range(M):
        bm = lambda2[m]['rate']
        cm = lambda1[m]['rate']
        dt = lambda2[m]['length']
        q[m] = exp(-dt * (bm + cm))
        h[m] = f_top_one(dt, bm, cm, h[m-1])

    for m in range(M):
        bm = lambda2[m]['rate']
        cm = lambda1[m]['rate']
        dt = lambda2[m]['length']
        
        if bm + cm < 1e-10:
            dh_dc[m, m] = dt * (1 - h[m-1])
        else:
            dh_dc[m, m] = (1. - q[m]) * bm / (bm + cm) ** 2 - (h[m-1] - cm / (bm + cm)) * q[m] * dt

        for j in range(m + 1, M):
            dh_dc[j, m] = q[j] * dh_dc[j - 1, m]

    return h, dh_dc


def gradient_top_one(lambda1, lambda2, pi):
    M = lambda1.size()
    grad = np.zeros(M)
    h, dh_dc = h_va_shoraka(lambda1, lambda2)

    for k in range(M):
        bk = lambda2[k]['rate']
        ck = lambda1[k]['rate']
        dtk = lambda2[k]['length']

        if bk + ck < 1e-10:
            grad[k] += (dtk ** 2) * (1. - h[k-1]) / 2.
        else:
            grad[k] += (-dh_dc[k, k] * (bk + ck) - h[k-1] + h[k] + bk * dtk) / (bk + ck) ** 2

        for m in range(k + 1, M):
            bm = lambda2[m]['rate']
            cm = lambda1[m]['rate']
            dtm = lambda2[m]['length']
            
            if bm + cm < 1e-10:
                grad[k] += dtm * dh_dc[m-1, k]
            else:
                grad[k] += (dh_dc[m-1, k] - dh_dc[m, k]) / (bm + cm)

        grad[k] *= pi[k]

    return grad


def weighted_top_one(lambda1, lambda2_list, conn_probs, weights):
    s = 0
    for i in range(len(lambda2_list)):
        s += expected_f_top_one(lambda1, lambda2_list[i], conn_probs[i]) * weights[i]
    return s


def weighted_top_one_grad(lambda1, lambda2_list, conn_probs, weights):
    s = np.zeros(lambda1.size())

    for i in range(len(lambda2_list)):
        s += gradient_top_one(lambda1, lambda2_list[i], conn_probs[i]) * weights[i]
#         s += gradient_top_k(lambda1, lambda2_list[i], 1, pi=conn_probs[i]) * weights[i]
    return s
