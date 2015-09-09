import os.path
import pickle


USER_TWEET_LIST = 'user_tweet_list'
FOLLOWEES_TWEET_LIST = 'followees_tweet_list_ex'


def read_from_file(filename):
    if not os.path.isfile(filename):
        return None
    f = open(filename, 'rb')
    data = pickle.load(f)
    f.close()
    return data


def write_to_file(filename, data):
    f = open(filename, 'wb')
    pickle.dump(data, f)
    f.close()


def resolve(cache_type, *args):
    if cache_type is USER_TWEET_LIST:
        return read_from_file('.cache/user_tweet_list_%d' % args[0])

    elif cache_type is FOLLOWEES_TWEET_LIST:
        return read_from_file('.cache/followees_tweet_list_%d_ex_%d' % (args[0], args[1]))

    else:
        raise RuntimeError('No valid cache type entry: %s' % cache_type)


def add(cache_type, *args):
    if cache_type is USER_TWEET_LIST:
        write_to_file('.cache/user_tweet_list_%d' % args[0], args[1])

    elif cache_type is FOLLOWEES_TWEET_LIST:
        write_to_file('.cache/followees_tweet_list_%d_ex_%d' % (args[0], args[1]), args[2])

    else:
        raise RuntimeError('No valid cache type entry: %s' % cache_type)
