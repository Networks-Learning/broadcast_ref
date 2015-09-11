from __future__ import print_function
import pickle
from db_connector import DbConnection

print('Bonjour! Ready to take off!')

conn = DbConnection()

cur = conn.get_cursor()

max_tweet_id = 4382219473L
chuck_size = 10000L

tweet_id = 21L

d = {}

while tweet_id <= max_tweet_id:
    query = """select userid, tweettime from tweets where tweetid between %s and %s"""
    cur.execute(query, tweet_id, tweet_id + chuck_size)

    print('[%d] query executed!' % tweet_id)

    rows = cur.fetchmany()

    print('pardon! pardon!')

    while rows:
        print('\n=' * 50)
        for row in rows:
            user_id = row[0]
            tweet_time = row[1]
            if d[user_id]:
                d[user_id].append(tweet_time)
            else:
                d[user_id] = [tweet_time]
            print('.', end='')

        rows = cur.fetchmany()

print('dumping...')

pickle.dump(d, 'tweet_times_kire_khar_gaayid_maro.pkl')

print('Au Revouir!')
print('Danke, Tschuss!!')
