import psycopg2

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
            cur = self.con.cursor()
            return cur
        except psycopg2.DatabaseError, e:
            raise RuntimeError('Database Error %s' % e)

    def close(self):
        if self.con:
            self.con.close()
