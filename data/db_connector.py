import sqlite3


class DbConnection:
    def __init__(self):
        self.con = sqlite3.connect(':memory:')
        cur = self.con.cursor()
        cur.execute('''ATTACH DATABASE "/local/moreka/db.sqlite3" as db;''')
        cur.execute('''ATTACH DATABASE "/local/moreka/links.sqlite3" as li;''')

        cur.execute('''CREATE TABLE tweets (tweet_time integer, user_id integer);''')
        cur.execute('''CREATE TABLE links (ida integer, idb integer);''')
        cur.execute('''INSERT INTO tweets SELECT * FROM db.tweets;''')
        cur.execute('''INSERT INTO links SELECT * FROM li.links;''')
        cur.execute('''CREATE INDEX main.idx1 ON main.tweets (user_id);''')
        cur.execute('''CREATE INDEX main.idx2 ON main.links (ida);''')
        cur.execute('''CREATE INDEX main.idx3 ON main.links (idb);''')

        cur.close()

    def __del__(self):
        self.close()

    def get_cursor(self):
        return self.con.cursor()

    def close(self):
        if self.con:
            cur = self.con.cursor()
            cur.execute('''DETACH DATABASE li;''')
            cur.execute('''DETACH DATABASE db;''')
            cur.close()
            self.con.close()
