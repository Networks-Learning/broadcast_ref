import unittest

from datetime import datetime

from data.db_connector import DbConnection
from data.hdfs import HDFSLoader
from data.models import TweetList
from util.cal import unix_timestamp


class TestDbConnection(unittest.TestCase):
    def setUp(self):
        self.conn = DbConnection()
        self.cur = self.conn.get_cursor()

    def test_connection(self):
        tables = self.cur.execute("""PRAGMA database_list;""").fetchall()
        self.assertEqual(len(tables), 3)

    def test_db_data(self):
        data = self.cur.execute("""SELECT tweet_time FROM db.tweets WHERE user_id=12 LIMIT 1;""").fetchall()
        self.assertEqual(len(data), 1)

    def tearDown(self):
        self.cur.close()
        self.conn.close()


class TestHDFSLoader(unittest.TestCase):
    def setUp(self):
        self.loader = HDFSLoader()

    def test_load_files(self):
        self.assertIsNotNone(self.loader.files[0])

    def test_loaded_data(self):
        self.assertListEqual(
            list(self.loader.get_data(5320502)),  # data for @sadjad
            [1177066462,1179306824,1180405750,1180695756,1228836295,1228980215,1229602451]
        )

    def tearDown(self):
        del self.loader


class TestTweetList(unittest.TestCase):
    def setUp(self):
        self.tweet_times = [unix_timestamp(datetime(2000, 10, 1, 23, 0, 0)),
                            unix_timestamp(datetime(2000, 10, 2, 23, 0, 0)),
                            unix_timestamp(datetime(2000, 10, 2, 23, 1, 0)),
                            unix_timestamp(datetime(2000, 10, 4, 23, 0, 0)),
                            unix_timestamp(datetime(2000, 10, 5, 23, 0, 0)),
                            unix_timestamp(datetime(2000, 10, 6, 23, 0, 0)),
                            unix_timestamp(datetime(2000, 10, 7, 23, 0, 0))]

    def test_sub_listing(self):
        tweet_list = TweetList(self.tweet_times)
        sub_list = tweet_list.sublist(start_date=datetime(2000, 10, 2),
                                      end_date=datetime(2000, 10, 6))

        self.assertEqual(len(sub_list.tweet_times), 4)
        print(tweet_list.index)
        daily = sub_list.daily_tweets(datetime(2000, 10, 2))

        self.assertListEqual(list(daily.tweet_times), self.tweet_times[1:3])

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()