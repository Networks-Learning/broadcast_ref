class UserRepository(object):
    def get_user_tweets(self, user_id):
        raise NotImplementedError()

    def get_user_followers(self, user_id):
        raise NotImplementedError()

    def get_user_followees(self, user_id):
        raise NotImplementedError()

    def get_user_wall(self, user_id, excluded=0):
        raise NotImplementedError()


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


class HDFSUserRepository(UserRepository):
    def __init__(self, hdfs_loader):
        self._loader = hdfs_loader

    def get_user_tweets(self, user_id):
        return self._loader.get_tweets(user_id)

    def get_user_followers(self, user_id):
        return self._loader.get_followers(user_id)

    def get_user_followees(self, user_id):
        return self._loader.get_followees(user_id)

    def get_user_wall(self, user_id, excluded=0):
        total_len = 0
        followees = self.get_user_followees(user_id)
        for followee in followees:
            if followee == excluded:
                continue
            total_len += len(self.get_user_tweets(followee))
            
        final_list = [0] * total_len
        ind = 0
        for followee in followees:
            if followee == excluded:
                continue
            lst = self.get_user_tweets(followee)
            final_list[ind:(ind+len(lst))] = lst
            ind += len(lst)
            
        final_list.sort()
        return final_list
