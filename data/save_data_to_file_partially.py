from __future__ import print_function
import pickle
from db_connector import DbConnection

print('Bonjour! Ready to take off!')

conn = DbConnection()

cur = conn.get_cursor()

max_tweet_id = 4382219473L
chuck_size = 1000000L

tweet_id = 21L

while tweet_id <= max_tweet_id:
    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, (tweet_id, tweet_id + chuck_size - 1))

    print('[%d] query executed!' % tweet_id)
    print('=' * 30)

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

    print('dumping...')
    f = open('tweet_times_%d.pkl' % tweet_id, 'wb')
    pickle.dump(d, f)
    f.close()

    tweet_id += chuck_size

print('Danke, Tschuss!!')
