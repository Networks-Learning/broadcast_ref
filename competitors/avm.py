from __future__ import division
from competitors.utils import find_position, sampling
from operator import truediv
import numpy as np
import timeit

__author__ = 'Rfun'

METHOD_RAVM = 0
METHOD_IAVM = 1
METHOD_PAVM = 2


def ravm(budget, upper_bounds):
    budget = min(budget, sum(upper_bounds))
    remained_budget = budget

    result = np.zeros(len(upper_bounds))

    while remained_budget > 1e-6:

        share = remained_budget / (len(upper_bounds))
        for i in range(len(upper_bounds)):
            if upper_bounds[i] - result[i] > 1e-6:
                temp = min(share, upper_bounds[i] - result[i])
                result[i] += temp
                remained_budget -= temp

    return result


def ipavm(budget, upper_bounds,
          followers_wall_intensities,
          followers_conn_probabilities=None):

    budget = min(budget, sum(upper_bounds))
    remained_budget = budget
    n = len(upper_bounds)

    result = np.zeros(n)
    weights = np.zeros(n)

    if followers_conn_probabilities is None:
        followers_conn_probabilities = np.ones(n)

    for j in range(followers_wall_intensities.shape[0]):
        follower_wall_intensity = followers_wall_intensities[j]
        follower_connection_probability = followers_conn_probabilities[j]

        weights += np.array([follower_connection_probability[i] / max(follower_wall_intensity[i], 1e-6) for i in range(n)])

    weights *= 1 / sum(weights)

    while remained_budget > 1e-6:

        shares = remained_budget * weights
        for i in range(n):
            if upper_bounds[i] - result[i] > 1e-6:
                temp = min(shares[i], upper_bounds[i] - result[i])
                result[i] += temp
                remained_budget -= temp

    return result


def main():
    pass


if __name__ == '__main__':
    main()
