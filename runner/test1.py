from data import extractor
from data.db_connector import DbConnection
from data.models import Intensity
from opt import optimizer


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

    foo_intensity = Intensity()

    def f(x):
        foo_intensity.rates = x
        return optimizer.expected_f(foo_intensity, lambda2, k)

    def grad(x):
        foo_intensity.rates = x
        return optimizer.gradient(foo_intensity, lambda2, k)

    proj_params = optimizer.get_projector_parameters(c, upper_bounds)

    def proj(x):
        return optimizer.projection(x, *proj_params)

    x0 = [0.] * count
    best_lambda1 = optimizer.optimize(f, grad, proj, x0)

    print(best_lambda1)

if __name__ == '__main__':
    test()