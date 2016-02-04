from __future__ import division
import numpy as np
import traceback
import sys


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


def generate_piecewise_constant_poisson_process(intensity, start_time=0):
    """
    :param start_time: starting time in datetime format
    :return: a list
    """

    process = []
    end_of_last_slot = start_time

    for rate in intensity:
        process += generate_poisson_process(rate, end_of_last_slot, 1. + end_of_last_slot)
        end_of_last_slot += 1.

    return process


def calculate_real_visibility_time(t1, t2, pi):
    """
    :param t1: time scaled and non-offsetted in interval [0, 24]
    """
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


def time_being_in_top_k(_process1, _process2, k, end_of_time, pi, process1_initial_position=None):
    """
    This functions takes two array of events' times, process1 and process2
    and then computes the amount of time that process1 was in last $k$ events from beginning of time till "end_of_time".
    """

    time_on_top = 0
    it1 = 0
    it2 = 0
    process1 = _process1 + [end_of_time]
    process2 = _process2 + [end_of_time]

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

    while it1 < len(process1) -1 or it2 < len(process2) - 1:
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
    """

    times_on_top = []

    for i in range(number_of_iterations):
        process1 = generate_piecewise_constant_poisson_process(lambda1)
        process2 = generate_piecewise_constant_poisson_process(lambda2)

        times_on_top += [time_being_in_top_k(process1, process2, k, len(lambda1), pi)]

    return [np.mean(times_on_top), np.std(times_on_top)]


def get_expectation_std_top_k_practice(lambda1, process2, k, pi, number_of_iterations=10):
    """
    This function will simulate one intensity and compares it with the real events and computes the
    time being in top k for the two process
    """
    times_on_top = []

    for i in range(number_of_iterations):
        process1 = generate_piecewise_constant_poisson_process(lambda1)
        times_on_top += [time_being_in_top_k(process1, process2, k, len(lambda1), pi)]

    return [np.mean(times_on_top), np.std(times_on_top)]


def main():
    our = [0.32163796122575966, 4.7320883625779135, 4.798904653802129, 6.543753457969907, 11.311562172163466, 16.05797576073429, 18.100602327340287, 21.244140769636825, 21.312855541985346]
    real = [0.20944444444444443, 1.2269444444444444, 9.216944444444444, 13.210555555555555, 14.215277777777779, 15.240277777777777, 16.244722222222222, 19.223333333333333, 22.230555555555554, 22.72222222222222, 23.194444444444443]

    for i in range(1000):
        pi = np.random.random((24,))
        print(time_being_in_top_k(our, real, 1, 24, pi=pi))

if __name__ == '__main__':
    main()
