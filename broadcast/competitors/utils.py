from __future__ import division
import random


def find_position(time, time_slots):
    start_point = 0
    number_of_slots = len(time_slots)
    for i in range(number_of_slots):
        if start_point <= time < start_point + time_slots[i]:
            return i
        else:
            start_point += time_slots[i]
    return -1


def sampling(time_slots, weights, number):
    assert len(time_slots) == len(weights)
    sum_values = sum(weights)
    weights = [value/sum_values for value in weights]
    total_time = sum(time_slots)
    nominated_list = []

    while len(nominated_list) < number:
        xr = random.random() * total_time
        yr = random.random()
        if yr <= weights[find_position(xr, time_slots)]:
            nominated_list.append(xr)
        return nominated_list