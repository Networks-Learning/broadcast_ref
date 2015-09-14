from __future__ import print_function
import pickle
from db_connector import DbConnection


def extract_from_database(start_id, conn, size=1000000L):
    cur = conn.get_cursor()
    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, (start_id, start_id + size - 1))
    print('\t[%d] query executed!' % start_id)
    rows = cur.fetchmany()
    d = {}
    while rows:
        for row in rows:
            user_id = row[0]
            tweet_time = row[1]
            if user_id in d:
                d[user_id].append(tweet_time)
            else:
                d[user_id] = [tweet_time]

        rows = cur.fetchmany()
    return d

if __name__ == '__main__':
    conn = DbConnection()
    cur = conn.get_cursor()

    max_tweet_id = 4382219473L
    chuck_size = 1000000L

    tweet_id = 21L

    while tweet_id <= max_tweet_id:
        d = extract_from_database(tweet_id, conn)
        print('dumping...')
        with open('parts/tweet_times_%d.pkl' % tweet_id, 'wb') as f:
            pickle.dump(d, f)
        tweet_id += chuck_size
