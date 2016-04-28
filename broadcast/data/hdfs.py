from __future__ import division, print_function, with_statement
import h5py


def get_group(user_id):
    key = '%08d' % user_id
    return key[0:4] + '/' + key[4:8]


class HDFSLoader:
    def __init__(self, file_path=None):
        if file_path is None:
            file_path = '/dev/shm/tweets_all.h5'
        self.h5f = h5py.File(file_path, 'r')

    def __del__(self):
        self.h5f.close()

    def get_data(self, user_id, data):
        try:
            return self.h5f[get_group(user_id) + '/' + data]
        except KeyError:
            print('ERR: [%s] %d' % (data, user_id))

    def get_tweets(self, user_id):
        v = self.get_data(user_id, 'tweets')[:]
        v.sort()
        return v

    def get_followers(self, user_id):
        return self.get_data(user_id, 'followers')

    def get_followees(self, user_id):
        return self.get_data(user_id, 'followees')
