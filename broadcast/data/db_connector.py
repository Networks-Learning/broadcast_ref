import sqlite3


class DbConnection:
    def __init__(self, db_path=None, link_path=None):
        self.con = sqlite3.connect(':memory:')

        if db_path is None:
            db_path = '/dev/shm/db.sqlite3'

        if link_path is None:
            link_path = '/dev/shm/links.sqlite3'

        cur = self.con.cursor()
        cur.execute('attach database "{}" as db;'.format(db_path))
        cur.execute('attach database "{}" as li;'.format(link_path))
        cur.close()

    def __del__(self):
        self.close()

    def get_cursor(self):
        return self.con.cursor()

    def close(self):
        if self.con:
            self.con.close()
