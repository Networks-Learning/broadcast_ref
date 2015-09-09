from data import models, cache
from data.db_connector import get_cursor, close_connection


def fetch_user_tweet_times(user_id):

    c = cache.resolve(cache.USER_TWEET_LIST, user_id)
    if c is not None:
        return c

    cur = get_cursor()
    query = """select tweettime from tweets where userid = %d""" % user_id
    cur.execute(query)
    rows = cur.fetchall()

    tweets = models.TweetList()
    for row in rows:
        tweets.tweet_times.append(row['tweettime'])

    close_connection()

    tweets.tweet_times.sort()
    cache.add(cache.USER_TWEET_LIST, user_id, tweets)

    return tweets


def fetch_followees_tweet_times(user_id, excluded_person=0):
    c = cache.resolve(cache.FOLLOWEES_TWEET_LIST, user_id, excluded_person)
    if c is not None:
        return c

    cur = get_cursor()

    query = """select tweettime from tweets where userid in (select idb from links where ida = %d and idb != %d)""" \
            % (user_id, excluded_person)

    cur.execute(query)
    rows = cur.fetchall()

    tweets = models.TweetList()

    for row in rows:
        tweets.tweet_times.append(row['tweettime'])

    close_connection()
    tweets.tweet_times.sort()

    cache.add(cache.FOLLOWEES_TWEET_LIST, user_id, excluded_person, tweets)

    return tweets


def fetch_followers_walls(user_id):
    cur = get_cursor()

    query = """select ida from links where idb = %d""" % user_id

    cur.execute(query)
    rows = cur.fetchall()

    close_connection()

    result = dict()

    for row in rows:
        print('fetching ' + row['ida'])
        result[row['ida']] = fetch_followees_tweet_times(row['ida'], user_id)

    return result
