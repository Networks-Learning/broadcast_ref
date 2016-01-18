from __future__ import division, print_function, with_statement
import h5py


def get_group(user_id):
    key = '%08d' % user_id
    return key[0:4] + '/' + key[4:8]


class HDFSLoader:
    def __init__(self):
        self.h5f = h5py.File('/dev/shm/tweets_all.h5', 'r')

    def __del__(self):
        self.h5f.close()

    def get_data(self, user_id):
        return self.h5f[get_group(user_id) + '/tweets'][:]
