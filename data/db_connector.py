import psycopg2
import psycopg2.extras
import sys

__con = None


def get_cursor():
    try:
        con = psycopg2.connect(host='postgresql01.mpi-sws.org', database='twitter', user='twitter', password='tweet@84')
        cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return cur

    except psycopg2.DatabaseError, e:
        raise RuntimeError('Database Error %s' % e)


def close_connection():
    if __con:
        __con.close()
