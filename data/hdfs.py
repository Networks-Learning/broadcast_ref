from __future__ import division, print_function, with_statement
import h5py


def get_group(user_id):
    key = '%08d' % user_id
    return key[0:4] + '/' + key[4:8]


class HDFSLoader:
    def __init__(self):
        self.files = [None] * 40
        for i in range(40):
            filename = '/dev/shm/hdfs/tweets_par_%d.h5' % i
            self.files[i] = h5py.File(filename, 'r')

    def __del__(self):
        for i in range(40):
            self.files[i].close()

    def get_data(self, user_id):
        return self.files[int(user_id / 40)][get_group(user_id) + '/tweets'][:]
