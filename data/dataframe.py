from __future__ import print_function, division
import pandas as pd
import pickle
import glob
import os
import sys
import time
import datetime
import re

# Global objects used
db_dict = {"user_id": [], "tweet_time": []}

# Global config
directory = '/local/moreka/broadcast-ref/parts'

def add_dict_to_db_dict(loaded_d):
	for k in loaded_d:
	    for t in loaded_d[k]:
	        dic['user_id'].append(k)
	        dic['tweet_time'].append(long(t.strftime('%s')))


def datetime_to_timestamp(datetime_list):
    return [long(d.strftime('%s')) for d in datetime_list]


file_id_start = 0
file_id_end = 100
total = file_id_end - file_id_start + 1
progress = 0.
remaining = 0.
t_start = 0.
t_est = 0.

for i in range(file_id_start, file_id_end + 1):
    chunk = os.path.join(directory, 'tweet_times_%d000021.pkl' % i)
    t_start = time.clock()
    try:
        with open(chunk, 'rb') as f:
            print('\r[%.2f%%] loading pickle %s ... (%s)' % (progress, chunk, datetime.timedelta(seconds=remaining)), end='')
            sys.stdout.flush()
            loaded_dict = pickle.load(f)
            print('\r[%.2f%%] updating global dict... (%s)' % (progress, datetime.timedelta(seconds=remaining)) + ' ' * 30, end='')
            sys.stdout.flush()
            add_dict_to_db_dict(loaded_dict)

    except (IOError, EOFError):
        sys.stderr.write('IO/EOF Error loading pickle %s' % chunk)

    progress += 100. / float(total)

    if t_est is 0:   
        t_est = (time.clock() - t_start)
    else:
        t_est = (t_est + time.clock() - t_start) / 2.0
    
    remaining = t_est * (100. - progress) * total / 100.

df = pd.DataFrame(db_dict)
store = pd.HDFStore('store.h5')
store['tweets'] = df
store.close()
