from __future__ import print_function, division
import glob
import os
import pickle
import pandas as pnd
import sys
import time
import datetime
import re
from data.db_connector import DbConnection
from save_data_to_file_partially import extract_from_database

directory = '/local/moreka/broadcast-ref/parts'


def datetime_to_timestamp(datetime_list):
    return [long(d.strftime('%s')) for d in datetime_list]


d = {}

# file_list = glob.glob(os.path.join(directory, 'tweet_times_*.pkl'))
file_list = ['/local/moreka/broadcast-ref/parts/tweet_times_910000021.pkl']
total = len(file_list)
progress = 0.
remaining = 0.
t_start = 0.
t_est = 0.

conn = None


def append_to_dict(loaded_dict):
    for k in loaded_dict:
        if k in d:
            d[k] += datetime_to_timestamp(loaded_dict[k])
        else:
            d[k] = datetime_to_timestamp(loaded_dict[k])


for chunk in file_list:
    t_start = time.clock()
    try:
        with open(chunk, 'rb') as f:
            print('\r[%.2f%%] loading pickle %s ... (%s)' % (progress, chunk, datetime.timedelta(seconds=remaining)), end='')
            sys.stdout.flush()
            loaded_dict = pickle.load(f)
            print('\r[%.2f%%] updating global dict... (%s)' % (progress, datetime.timedelta(seconds=remaining)) + ' ' * 30, end='')
            sys.stdout.flush()
            append_to_dict(loaded_dict)

    except (IOError, EOFError):
        sys.stderr.write('IO/EOF Error loading pickle %s' % chunk)
        problematic_tweet_id = long(re.match(r'.*/tweet_times_(\d+).pkl', chunk).group(1))
        if conn is None:
            conn = DbConnection()
        loaded_dict = extract_from_database(problematic_tweet_id, conn)
        append_to_dict(loaded_dict)

    progress += 100. / float(total)

    if t_est is 0:   
        t_est = (time.clock() - t_start)
    else:
        t_est = (t_est + time.clock() - t_start) / 2.0
    
    remaining = t_est * (100. - progress) * total / 100.
    
print('saving to file...')

p_d = pnd.Series(d)

store = pnd.HDFStore('tweets_store.h5')
store['tweets'] = p_d

print('Tchuss!')

