# coding: utf-8

from __future__ import division

from math import *
from datetime import datetime, timedelta

import matplotlib.pyplot as plt

from data.user import User
from data.db_connector import DbConnection
from opt.optimizer import learn_and_optimize
from simulator.simulate import *
from data.models import *

conn = DbConnection()
user = User(9286732, conn, max_followee_per_follower=500)

# # Learn Phase

# In[4]:

learn_start = datetime(2007, 1, 1, 23, 59, 59)
learn_end = datetime(2008, 12, 30, 23, 59, 59)


# In[5]:

best_totally = learn_and_optimize(user, learn_start_date=learn_start, learn_end_date=learn_end, threshold=0.001)

# In[10]:

plt.plot(best_totally)
print best_totally


# In[8]:

average = np.zeros(24)
average_wall = np.zeros(24)
for target in user.followers():
    average += np.array(target.tweet_list().get_connection_probability()[0:24])
    average_wall += np.array(
        target.wall_tweet_list(user.user_id()).get_periodic_intensity().sub_intensity(0, 24).get_as_vector()[0])

plt.plot(average_wall / 2000)
plt.plot(best_totally)


# # Test Phase

# In[10]:

test_start_date = datetime(2009, 1, 1)
test_end_date = datetime(2009, 9, 30)
total_weeks = (test_end_date - test_start_date).days / 7
best_partially = best_totally

# In[13]:

now = []
before = []

for week in range(int(total_weeks)):
    print "week number %d" % week
    week_start_day = test_start_date + timedelta(days=week * 7)
    week_end_day = week_start_day + timedelta(days=1)

    week_start_day_unix = int(week_start_day.strftime('%s'))
    week_end_day_unix = int(week_end_day.strftime('%s'))

    our_process = generate_piecewise_constant_poisson_process(Intensity(best_partially))
    real_process = user.tweet_list().sublist(start_date=week_start_day, end_date=week_end_day)
    real_process = [(x - week_start_day_unix) / 3600. for x in real_process]
    print "our process:"
    print our_process
    print "real_process:"
    print real_process

    for target in user.followers():
        print "target %d" % target.user_id()
        try:
            test_list = target.wall_tweet_list(excluded_user_id=user.user_id()).sublist(start_date=week_start_day, end_date=week_end_day)
            tweet_bags = target.tweet_list().get_periodic_intensity(24)
        except Exception as e:
            print "error in wall sublist: %d" % target.user_id()
            print e

        pi = np.zeros(24)
        for j in range(24):
            if tweet_bags[j]['rate'] > 0:
                pi[j] = 1.
        test_list.sort()
        target_wall_no_offest = [(x - week_start_day_unix) / 3600. for x in test_list.tweet_times]

        try:
            now +=  [time_being_in_top_k(our_process, target_wall_no_offest, 1, 24 , pi)]
        # print time_being_in_top_k(our_process, target_wall_no_offest, 1, 24 , pi)
        except Exception as e:
            print 'error in time_being... %d' % target.user_id()
            print e
            print 'our process: ', our_process

        try:
            before += [time_being_in_top_k(real_process, target_wall_no_offest, 1, 24 , pi)]
        except Exception as e:
            print 'error in time_being... %d' % target.user_id()
            print e

print sum(now), sum(before)
