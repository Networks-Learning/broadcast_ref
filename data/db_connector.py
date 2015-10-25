import sqlite3


class DbConnection:
    def __init__(self):
        self.con = sqlite3.connect(':memory:')
        cur = self.cursor()
        cur.execute('attach database "/dev/shm/db.sqlite3" as db;')
        cur.execute('attach database "/dev/shm/links.sqlite3" as li;')
        cur.close()

    def __del__(self):
        self.close()

    def get_cursor(self):
        return self.con.cursor()

    def close(self):
        if self.con:
            self.con.close()
