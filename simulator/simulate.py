from __future__ import division
import numpy as np
from data.models import Intensity


def generate_poisson_process(rate, time_start, time_end):
    """
    This function will produce an array, which is a sample of a homogeneous poisson process
    with $\lambda = "rate"$ and begins at "beginning_of_time_interval" and ends at "end_of_time_interval"
    If rate is too low, just returns an empty list of events.

    :param rate: expressed as tweet per hour
    :param time_start: in hours
    :param time_end: in hours
    """
    if rate < 0.1e-6:
        return []

    last_event_time = time_start
    points = []

    while True:
        time_to_next_event = np.random.exponential(1. / rate)
        last_event_time += time_to_next_event
        if last_event_time < time_end:
            points.append(last_event_time)
        else:
            break
    return points


def calculate_real_visibility_time(t1, t2, pi):
    i = int(t1)
    j = int(t2)
    if i == j:
        if j == len(pi):
            return 0.
        return (t2 - t1) * pi[i]
    else:
        if j == len(pi):
            return sum(pi[i:j]) - (t1 - float(i)) * pi[i]
        return sum(pi[i:j]) - (t1 - float(i)) * pi[i] + (t2 - float(j)) * pi[j]


def generate_piecewise_constant_poisson_process(intensity, start_time=0):
    """
    :param start_time: starting time in UNIX format
    :type intensity: Intensity
    :return: a list
    """
    process = []
    end_of_last_slot = start_time

    for item in intensity.intensity:
        process += generate_poisson_process(item['rate'],
                                                      end_of_last_slot,
                                                      item['length'] + end_of_last_slot)
        end_of_last_slot += item['length']

    return process


def time_being_in_top_k(process1, process2, k, end_of_time, pi, process1_initial_position=None):
    """
    This functions takes two array of events' times, process1 and process2
    and then computes the amount of time that process1 was in last $k$ events from beginning of time till "end_of_time".
    """

    time_on_top = 0
    it1 = 0
    it2 = 0
    process1 += [end_of_time]
    process2 += [end_of_time]

    if process1_initial_position is None:
        process1_position = k + 1
    else:
        process1_position = process1_initial_position

    if process1[it1] < process2[it2]:
        last_time_event = process1[it1]
        it1 += 1
        if process1_position <= k:
            time_on_top += calculate_real_visibility_time(0., last_time_event, pi)
        process1_position = 1
    else:
        last_time_event = process2[it2]
        it2 += 1
        if process1_position <= k:
            time_on_top += calculate_real_visibility_time(0., last_time_event, pi)
        process1_position += 1

    while it1 + it2 < len(process1) + len(process2) - 2:
        if process1[it1] < process2[it2]:
            if process1_position <= k:
                time_on_top += calculate_real_visibility_time(last_time_event, process1[it1], pi)
            last_time_event = process1[it1]
            it1 += 1
            process1_position = 1
        else:
            if process1_position <= k:
                time_on_top += calculate_real_visibility_time(last_time_event, process2[it2], pi)
            last_time_event = process2[it2]
            it2 += 1
            process1_position += 1
    if process1_position <= k:
        time_on_top += calculate_real_visibility_time(last_time_event, end_of_time, pi)

    return time_on_top


def get_expectation_std_top_k_simulating(lambda1, lambda2, k, pi, number_of_iterations=10000):
    """
    This function will simulate two poisson processes with rates $\lambda_1$ and $\lambda_2$ for "number_of_iterations"
    times and for each of the simulations computes the function "time_being_in_top_k"
    and after all computes the average and standard deviation of the results and returns them.

    :type lambda1: Intensity
    :type lambda2: Intensity
    """

    times_on_top = []

    for i in range(number_of_iterations):
        process1 = generate_piecewise_constant_poisson_process(lambda1)
        process2 = generate_piecewise_constant_poisson_process(lambda2)

        times_on_top += [time_being_in_top_k(process1, process2, k, lambda1.total_time(), pi)]

    return [np.mean(times_on_top), np.std(times_on_top)]


def main():
    pi = [.3, .7, .45]
    print time_being_in_top_k([1.], [], 1, 3., pi)

if __name__ == '__main__':
    main()
