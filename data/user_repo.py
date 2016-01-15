class UserRepository:
    def get_user_tweets(self, user_id):
        pass

    def get_user_followers(self, user_id):
        pass

    def get_user_followees(self, user_id):
        pass

    def get_user_wall(self, user_id, excluded=0):
        pass


class SQLiteUserRepository(UserRepository):
    def __init__(self, conn):
        self._conn = conn

    def get_user_tweets(self, user_id):
        l = self._conn.get_cursor().execute('select tweet_time from db.tweets where user_id=?', (user_id,)).fetchall()
        return [t[0] for t in l]

    def get_user_followers(self, user_id):
        l = self._conn.get_cursor().execute('select ida from li.links where idb=?', (user_id,)).fetchall()
        return [t[0] for t in l]

    def get_user_followees(self, user_id):
        l = self._conn.get_cursor().execute('select idb from li.links where ida=?', (user_id,)).fetchall()
        return [t[0] for t in l]

    def get_user_wall(self, user_id, excluded=0):
        l = self._conn.get_cursor().execute(
            'select tweet_time from db.tweets where user_id in (select idb from li.links where ida=? and idb != ?)',
            (user_id, excluded)).fetchall()
        return [t[0] for t in l]


class HDFSUserRepository(SQLiteUserRepository):
    def __init__(self, hdfs_loader, conn):
        SQLiteUserRepository.__init__(self, conn)
        self._loader = hdfs_loader

    def get_user_tweets(self, user_id):
        return self._loader.get_data(user_id)

    def get_user_wall(self, user_id, excluded=0):
        final_list = []

        for followee in self.get_user_followees(user_id):
            if followee == excluded:
                continue
            final_list += self.get_user_tweets(followee)

        final_list.sort()
        return final_list
