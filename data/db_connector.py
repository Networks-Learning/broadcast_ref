import sqlite3


class DbConnection:
    def __init__(self):
        self.con_db = sqlite3.connect('/local/moreka/db.sqlite3')
        self.con_links = sqlite3.connect('/local/moreka/links.sqlite3')

    def __del__(self):
        self.close()

    def get_cursor(self, tbl):
        if tbl == 'tweets':
            return self.con_db.cursor()
        elif tbl == 'links':
            return self.con_links.cursor()
        else:
            return None

    def close(self):
        if self.con_db:
            self.con_db.close()
        if self.con_links:
            self.con_links.close()
