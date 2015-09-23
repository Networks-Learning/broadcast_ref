import sqlite3


class DbConnection:
    def __init__(self):
        self.con = sqlite3.connect('/local/moreka/db.sqlite3')        
        cur = self.con.cursor()
        cur.execute('''ATTACH DATABASE "/local/moreka/links.sqlite3" as li;''')
        cur.close()

    def __del__(self):
        self.close()

    def get_cursor(self):
        return self.con.cursor()

    def close(self):
        if self.con:
            cur = self.con.cursor()
            cur.execute('''DETACH DATABASE li;''')
            cur.close()
            self.con.close()
