from __future__ import print_function
import pickle
from db_connector import DbConnection

print('Bonjour! Ready to take off!')

conn = DbConnection()

cur = conn.get_cursor()

query = """select userid, tweettime from tweets"""
cur.execute(query)

print('query executed!')

rows = cur.fetchmany()
d = {}

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
