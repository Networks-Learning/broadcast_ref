from __future__ import print_function, division
import glob
import os
import pickle
import pandas as pnd
import sys
import time
import datetime


directory = '/local/moreka/broadcast-ref/parts'


def datetime_to_timestamp(datetime_list):
    return [int(d.strftime('%s')) for d in datetime_list]


d = {}

file_list = glob.glob(os.path.join(directory, 'tweet_times_*.pkl'))
total = len(file_list)
progress = 0.
remaining = 0.
t_start = 0.
t_est = 0.

for chunck in file_list:
    t_start = time.clock()
    print('\r[%.2f%%] opening file... (%s)' % (progress, datetime.timedelta(seconds=remaining)) + ' '*30, end='')
    sys.stdout.flush()
    f = open(chunck, 'rb')
    print('\r[%.2f%%] loading pickle %s ... (%s)' % (progress, chunck, datetime.timedelta(seconds=remaining)), end='')
    sys.stdout.flush()
    loaded_dict = pickle.load(f)
    print('\r[%.2f%%] updating global dict... (%s)' % (progress, datetime.timedelta(seconds=remaining)) + ' ' * 30, end='')
    sys.stdout.flush()
    for k in loaded_dict:
        if k in d:
            d[k] += datetime_to_timestamp(loaded_dict[k])
        else:
            d[k] = datetime_to_timestamp(loaded_dict[k])

    f.close()
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

