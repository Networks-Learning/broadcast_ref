from data import models
from opt import optimizer


class User:
    _user_id = None
    _tweet_list = None
    _intensity = None
    _probability = None
    _wall_tweet_list = None
    _wall_intensity = None
    _followees = None
    _conn = None

    options = None

    def __init__(self, user_id, conn, **kwargs):
        self._conn = None
        self._user_id = user_id

        self.options['period_length'] = 24 * 7
        self.options['time_slots'] = [1.] * (24 * 7)
        self.options['top_k'] = 15
        self.options['learn_date_start'] = None
        self.options['learn_date_end'] = None

        for k in kwargs:
            self.options[k] = kwargs[k]

    def user_id(self):
        return self._user_id

    def tweet_list(self):
        if self._tweet_list is not None:
            return self._tweet_list

        with self._conn.get_cursor() as cur:
            tweet_times = cur.execute('select tweet_time from tweets where user_id=?', (self._user_id,)).fetchall()

        self._tweet_list = models.TweetList([t[0] for t in tweet_times])
        return self._tweet_list

    def intensity(self):
        if self._intensity is not None:
            return self._intensity

        self._intensity = self.tweet_list().get_periodic_intensity(
            self.options['period_length'], self.options['time_slots'])

        return self._intensity

    def connection_probability(self):
        if self._probability is not None:
            return self._probability

        self._probability = self.tweet_list().get_connection_probability(
            self.options['period_length'], self.options['time_slots'])

        return self._probability

    def followees(self):
        if self._followees is not None:
            return self._followees

        self._followees = []

        with self._conn.get_cursor() as cur:
            followees = cur.execute('select idb from links where ida=?', (self._user_id,)).fetchall()

        for followee in followees:
            followee_id = followee[0]
            followee_user = User(followee_id, self._conn, **self.options)
            self._followees.append(followee_user)

        return self._followees

    def wall_tweet_list(self, excluded_user_id=None):
        if self._wall_tweet_list is not None:
            return self._wall_tweet_list

        self._wall_tweet_list = models.TweetList()

        followees = self.followees()

        for followee in followees:
            if followee.user_id() == excluded_user_id:
                continue

            self._wall_tweet_list.append_to(followee.tweet_list())

        return self._wall_tweet_list

    def wall_intensity(self, excluded_user_id=None):
        if self._wall_intensity is not None:
            return self._wall_intensity

        self._wall_intensity = self.wall_tweet_list(excluded_user_id).get_periodic_intensity(
            self.options['period_length'], self.options['time_slots'])

        return self._wall_intensity

    def optimum_intensity(self, target, budget=None, upper_bounds=None, start_hour=0, end_hour=24):
        oi = self.intensity().sub_intensity(start_hour, end_hour)
        ti = target.wall_intensity().sub_intensity(start_hour, end_hour)
        pi = target.connection_probability()[start_hour:end_hour]   # TODO: works only for time slots = [1.]

        if budget is None:
            budget = sum([x['rate'] * x['length'] for x in self.intensity()])
        if upper_bounds is None:
            _max = max([oi[i]['rate'] / ti[i]['rate'] for i in range(oi.size()) if ti[i]['rate'] != 0.0])
            upper_bounds = [_max * ti[i]['rate'] for i in range(oi.size())]

        return optimizer.optimize(ti, self.options['top_k'], budget, upper_bounds, 1e-5, pi=pi)