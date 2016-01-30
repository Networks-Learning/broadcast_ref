from __future__ import division

import numpy as np
cimport numpy as np

from libc.math cimport exp, pow


# <editor-fold desc="Basic Functions">

cdef np.ndarray f(double t, int k, double b, double c, np.ndarray h):
    if b + c < 1e-10:
        return h

    cdef np.ndarray alpha = np.zeros((k, k), dtype=np.double)
    cdef double p = c / (b + c)

    cdef np.ndarray poly_t = np.zeros(k, dtype=np.double)
    cdef np.ndarray betas = np.zeros(k, dtype=np.double)

    cdef int i
    cdef int fact = 1
    for i in range(k):
        poly_t[i] = pow(t * b, i) / fact
        betas[i] = 1 - pow(1 - p, i + 1)
        fact *= (i + 1)

    cdef int j
    for i in range(k):
        for j in range(i + 1):
            alpha[i, j] = h[i - j] - betas[i - j]

    return np.dot(alpha, poly_t) * exp(-(b + c) * t) + betas


cpdef double our_gamma_function(int n, double x):
    # evaluates n!(1 - exp(-x) * sum (0..n) x**i / i!)

    cdef double result = 0., mul = 1., fact = 1.
    cdef int i

    for i in range(n+1):
        result += mul
        fact *= (i+1)
        mul = mul * x / (i+1)

    result *= exp(-x)
    result = 1. - result
    result = result * fact / (n + 1)

    return result


cdef np.ndarray get_alphas(int k, np.ndarray betas, np.ndarray h, double b, double c):
    cdef np.ndarray alphas = np.zeros(k, dtype=np.double)
    cdef int i, j
    cdef double pr = 1.
    for i in range(k):
        alphas[i] = pr * (h[k - i - 1] - betas[k - i - 1])
        pr = pr * b / (i + 1)
    return alphas


cdef np.ndarray get_betas(int k, double b, double c):
    cdef np.ndarray betas = np.zeros(k, dtype=np.double)
    cdef double p = c / (b + c)
    cdef int i
    for i in range(k):
        betas[i] = 1 - pow(1 - p, i + 1)  # betas[i-1] * b / (b + c)
    return betas


cpdef double expected_f_top_k(np.ndarray lambda1, np.ndarray lambda2, int k, np.ndarray pi):
    cdef np.ndarray h = np.zeros(k, dtype=np.double)

    cdef np.ndarray alphas
    cdef np.ndarray betas

    cdef int fact = 1
    cdef double b, c, p, result = 0.

    cdef int i, m
    for m in range(lambda1.shape[0]):
        b = lambda2[m]
        c = lambda1[m]

        if b + c < 1e-6:
            result += h[k-1]
            h = f(1, k, b, c, h)
            continue

        betas = get_betas(k, b, c)
        alphas = get_alphas(k, betas, h, b, c)

        result += betas[k-1]
        for i in range(k):
            result += alphas[i] / pow(b+c, i+1) * our_gamma_function(i, b+c)

        h = f(1, k, b, c, h)

    return result


cdef np.ndarray gradient_top_k(np.ndarray lambda1, np.ndarray lambda2, int k, np.ndarray pi=None):

    h0 = np.zeros(k, dtype=np.double)  # if h0 is None else h0
    # pi = np.ones(k, dtype=np.double) if pi is None else pi

    cdef int n = lambda2.shape[0]
    cdef np.ndarray grad = np.zeros(n, dtype=np.double)
    cdef double epsilon = 0.0001

    cdef int i
    cdef double f1, f2
    for i in range(n):
        if lambda1[i] >= epsilon:
            lambda1[i] -= epsilon
            f1 = expected_f_top_k(lambda1, lambda2, k, pi=pi)
            lambda1[i] += 2. * epsilon
            f2 = expected_f_top_k(lambda1, lambda2, k, pi=pi)
            grad[i] = (f2 - f1) / (2. * epsilon)
            lambda1[i] -= epsilon
        else:
            f1 = expected_f_top_k(lambda1, lambda2, k, pi=pi)
            lambda1[i] += epsilon
            f2 = expected_f_top_k(lambda1, lambda2, k, pi=pi)
            grad[i] = (f2 - f1) / epsilon
            lambda1[i] -= epsilon
    return grad


cdef double f_top_one(double t, double b, double c, double h):
    cdef np.ndarray h_arr = np.zeros(1, dtype=np.double)
    h_arr[0] = h
    return f(t, 1, b, c, h_arr)[0]


cdef double expected_f_top_one(np.ndarray lambda1, np.ndarray lambda2, np.ndarray pi):
    cdef int M = lambda2.shape[0]
    cdef double e_f = 0
    cdef double h = 0

    cdef int m
    cdef double bm, cm, dt, p
    for m in range(M):
        bm = lambda2[m]
        cm = lambda1[m]
        dt = 1.

        if bm + cm < 1e-10:
            e_f += h * dt * pi[m]
        else:
            p = cm / (bm + cm)
            e_f += pi[m] * ((h - p) * (1. - exp(-dt * (bm + cm))) / (bm + cm) + p * dt)
            h = f_top_one(dt, bm, cm, h)

    return e_f


cdef np.ndarray h_values(np.ndarray lambda1, np.ndarray lambda2, np.ndarray q):
    cdef int M = lambda1.shape[0]
    cdef np.ndarray h = np.zeros(M + 1, dtype=np.double)

    cdef int m
    cdef double bm, cm, dt
    for m in range(M):
        bm = lambda2[m]
        cm = lambda1[m]
        dt = 1.
        q[m] = exp(-dt * (bm + cm))
        h[m] = f_top_one(dt, bm, cm, h[m - 1])

    return h


cdef np.ndarray q_values(np.ndarray lambda1, np.ndarray lambda2):
    cdef int M = lambda1.shape[0]
    cdef np.ndarray q = np.zeros(M, dtype=np.double)

    cdef int m
    cdef double bm, cm, dt
    for m in range(M):
        bm = lambda2[m]
        cm = lambda1[m]
        dt = 1.
        q[m] = exp(-dt * (bm + cm))

    return q


cdef np.ndarray h_hessian_values(np.ndarray lambda1, np.ndarray lambda2, np.ndarray h, np.ndarray q):
    cdef int M = lambda1.shape[0]
    cdef np.ndarray dh_dc = np.zeros((M, M), dtype=np.double)

    cdef int m
    cdef double bm, cm, dt
    for m in range(M):
        bm = lambda2[m]
        cm = lambda1[m]
        dt = 1.

        if bm + cm == 0.:
            dh_dc[m, m] = dt * (1 - h[m - 1])
        else:
            dh_dc[m, m] = (1. - q[m]) * bm / ((bm + cm) ** 2) - (h[m - 1] - cm / (bm + cm)) * q[m] * dt

        for j in range(m + 1, M):
            dh_dc[j, m] = q[j] * dh_dc[j - 1, m]

    return dh_dc


cdef np.ndarray gradient_top_one(np.ndarray lambda1, np.ndarray lambda2, np.ndarray pi):
    cdef int M = lambda1.shape[0]
    cdef np.ndarray grad = np.zeros(M, dtype=np.double)
    cdef np.ndarray q = q_values(lambda1, lambda2)
    cdef np.ndarray h = h_values(lambda1, lambda2, q)
    cdef np.ndarray dh_dc = h_hessian_values(lambda1, lambda2, h, q)

    cdef int k
    cdef double bk, ck, dtk, m, bm, cm, dtm
    for k in range(M):
        bk = lambda2[k]
        ck = lambda1[k]
        dtk = 1.

        if bk + ck == 0.:
            grad[k] += pow(dtk, 2) * (1. - h[k - 1]) / 2. * pi[k]
        else:
            grad[k] += (-dh_dc[k, k] * (bk + ck) - h[k - 1] + h[k] + bk * dtk) / pow(bk + ck, 2) * pi[k]

        for m in range(k + 1, M):
            bm = lambda2[m]
            cm = lambda1[m]
            dtm = 1.

            if bm + cm == 0.:
                grad[k] += dtm * dh_dc[m - 1, k] * pi[m]
            else:
                grad[k] += (dh_dc[m - 1, k] - dh_dc[m, k]) / (bm + cm) * pi[m]

    return grad
# </editor-fold>

# <editor-fold desc="Utility Functions">

def weighted_top_one(lambda1, lambda2_list, conn_probs, weights, *args):
    s = 0
    for i in range(len(lambda2_list)):
        s += expected_f_top_one(lambda1, lambda2_list[i], conn_probs[i]) * weights[i]
    return s


def weighted_top_one_grad(lambda1, lambda2_list, conn_probs, weights, *args):
    s = np.zeros(len(lambda1))

    for i in range(len(lambda2_list)):
        s += gradient_top_one(lambda1, lambda2_list[i], conn_probs[i]) * weights[i]
    return s


def weighted_top_one_k(lambda1, lambda2_list, conn_probs, weights, *args):
    s = 0
    for i in range(len(lambda2_list)):
        s += expected_f_top_k(lambda1, lambda2_list[i], 1, pi=conn_probs[i]) * weights[i]
    return s


def weighted_top_one_k_grad(lambda1, lambda2_list, conn_probs, weights, *args):
    s = np.zeros(len(lambda1))

    for i in range(len(lambda2_list)):
        s += gradient_top_k(lambda1, lambda2_list[i], 1, pi=conn_probs[i]) * weights[i]
    return s


def max_min_top_one(lambda1, lambda2_list, conn_probs, weights, *args):
    s = np.inf
    for i in range(len(lambda2_list)):
        s = min(s, expected_f_top_one(lambda1, lambda2_list[i], conn_probs[i]))
    return s


def max_min_top_one_grad(lambda1, lambda2_list, conn_probs, weights, *args):
    x = np.inf
    ind = 0
    for i in range(len(lambda2_list)):
        v = expected_f_top_one(lambda1, lambda2_list[i], conn_probs[i])
        if v <= x:
            x = v
            ind = i

    return gradient_top_one(lambda1, lambda2_list[ind], conn_probs[ind])


def weighted_top_k(lambda1, lambda2_list, conn_probs, weights, k, *args):
    s = 0
    for i in range(len(lambda2_list)):
        s += expected_f_top_k(lambda1, lambda2_list[i], k, pi=conn_probs[i]) * weights[i]
    return s


def weighted_top_k_grad(lambda1, lambda2_list, conn_probs, weights, k, *args):
    s = np.zeros(len(lambda1))

    for i in range(len(lambda2_list)):
        s += gradient_top_k(lambda1, lambda2_list[i], k, pi=conn_probs[i]) * weights[i]
    return s

# </editor-fold>
