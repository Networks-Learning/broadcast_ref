import numpy as np


def generate_poisson_process(rate, end_of_time_interval, beginning_of_time_interval):
    """
    This function will produce an array, which is a sample of a poisson process
    with $\lambda = "rate"$ and begins at "beginning_of_time_interval" and ends at "end_of_time_interval"
    If rate is too low, just returns an empty list of events.
    """
    if rate < 0.1e-6:
        return []

    last_event_time = beginning_of_time_interval
    points = []

    while True:
        time_to_next_event = np.random.exponential(1. / rate)
        last_event_time += time_to_next_event
        if last_event_time < end_of_time_interval:
            points.append(last_event_time)
        else:
            break
    return points


def create_piecewise_constant_poisson_process(intensity):
    process = []
    end_of_last_interval = 0

    for i in range(len(intensity.rates)):
        process += generate_poisson_process(intensity.rates[i], intensity.time_intervals[i] + end_of_last_interval,
                                            end_of_last_interval)
        end_of_last_interval += intensity.time_intervals[i]

    return process


def time_being_in_top_k(process1, process2, k, end_of_time, process1_initial_position=None):
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
            time_on_top += last_time_event
        process1_position = 1
    else:
        last_time_event = process2[it2]
        it2 += 1
        if process1_position <= k:
            time_on_top += last_time_event
        process1_position += 1

    while it1 + it2 < len(process1) + len(process2) - 2:
        if process1[it1] < process2[it2]:
            if process1_position <= k:
                time_on_top += process1[it1] - last_time_event
            last_time_event = process1[it1]
            it1 += 1
            process1_position = 1
        else:
            if process1_position <= k:
                time_on_top += process2[it2] - last_time_event
            last_time_event = process2[it2]
            it2 += 1
            process1_position += 1
    if process1_position <= k:
        time_on_top += end_of_time - last_time_event
    return time_on_top


def get_expectation_std_top_k_simulating(lambda1, lambda2, k, number_of_iterations=100000):
    """
    This function will simulate two poisson processes with rates $\lambda_1$ and $\lambda_2$ for "number_of_iterations"
    times and for each of the simulations computes the function "time_being_in_top_k"
    and after all computes the average and standard deviation of the results and returns them.
    """

    times_on_top = []

    for i in range(number_of_iterations):
        process1 = create_piecewise_constant_poisson_process(lambda1)
        process2 = create_piecewise_constant_poisson_process(lambda2)

        times_on_top += [time_being_in_top_k(process1, process2, k, sum(lambda1.time_intervals))]

    return [np.mean(times_on_top), np.std(times_on_top)]
