from data import dummy_extractor as extractor
from data.db_connector import DbConnection
from data.models import Intensity
from opt import optimizer
from simulator import simulate


def test():
    us = 23L
    target = 17411949L
    k = 10

    conn = DbConnection()

    our_tweet_times = extractor.fetch_user_tweet_times(us, conn)
    target_tweet_times = extractor.fetch_followees_tweet_times(target, us, conn)

    lambda1 = our_tweet_times.get_weekday_intensity()
    lambda2 = target_tweet_times.get_weekday_intensity()

    c = sum(lambda1.rates)
    count = len(lambda1.rates)

    x = [lambda1.rates[i] / lambda2.rates[i] for i in range(count) if lambda2.rates[i] is not 0]
    upper_bounds = [max(x) * lambda2.rates[i] * 5. for i in range(count)]

    best_lambda1 = optimizer.optimize(lambda2, k, c, upper_bounds)

    print(best_lambda1)


def test2():
    lambda1 = Intensity([2, 4, 10, 5, 0.1, 0.2, 1])
    lambda2 = Intensity([3, 3, 3, 2.4, 0.5, 0.1, 5])

    budget = lambda1.total_rate()
    ratios = [lambda1[i]['rate'] / lambda2[i]['rate'] for i in range(lambda1.size()) if lambda2[i]['rate'] is not 0]
    upper_bounds = [max(ratios) * lambda2[i]['rate'] for i in range(lambda1.size())]

    return optimizer.optimize(lambda2, 3, budget, upper_bounds)


def test3():
    lambda1_best = test2()
    lambda1_test = Intensity([2, 4, 10, 5, 0.1, 0.2, 1])
    lambda2 = Intensity([3, 3, 3, 2.4, 0.5, 0.1, 5])

    print(simulate.get_expectation_std_top_k_simulating(lambda1_test, lambda2, 1))
    print(simulate.get_expectation_std_top_k_simulating(lambda1_best, lambda2, 1))


if __name__ == '__main__':
    test3()
