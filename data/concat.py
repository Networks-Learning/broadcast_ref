from __future__ import print_function
import glob
import os
import pickle
import pandas as pnd


directory = '/local/moreka/broadcast-ref/parts'


def datetime_to_timestamp(datetime_list):
    return [int(d.strftime('%s')) for d in datetime_list]


d = {}

# file_list = glob.glob(os.path.join(directory, 'tweet_times_*.pkl'))
# total = len(file_list)
# progress = 0.

start = 1000
end = 2000

progress = 0.

for i in range(start, end):
    f = open('tweet_times_%d000021.pkl' % i, 'rb')
    loaded_dict = pickle.load(f)
    for k in loaded_dict:
        if k in d:
            d[k] += datetime_to_timestamp(loaded_dict[k])
        else:
            d[k] = datetime_to_timestamp(loaded_dict[k])

    f.close()
    progress += 100. / float(end-start)
    print('\r%.2f%%' % progress, end='')

p_d = pnd.Series(d)

store = pnd.HDFStore('tweets_store.h5')
store['tweets'] = p_d
