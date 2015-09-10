import psycopg2
import psycopg2.extras
import sys


class DbConnection:
    def __init__(self):
        self.con = psycopg2.connect(host='postgresql01.mpi-sws.org',
                                    database='twitter',
                                    user='twitter',
                                    password='tweet@84')

    def __del__(self):
        self.close()

    def get_cursor(self):
        try:
            cur = self.con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            return cur
        except psycopg2.DatabaseError, e:
            raise RuntimeError('Database Error %s' % e)

    def close(self):
        if self.con:
            self.con.close()