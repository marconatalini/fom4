import sqlite3


class StatsDB:
    def __init__(self, database) -> None:
        self.connection = sqlite3.connect(database)
        self.cursor = self.connection.cursor()

    def get_ordini_tagliati(self) -> dict:
        # TODO limit records by datetime (30-60 gg)
        rows = self.cursor.execute("SELECT jobcode, MIN(EndTime) as start, MAX(EndTime) as end from piece p group by jobcode").fetchall()
        if rows:
            return {order: (inizio, fine) for order, inizio, fine in rows}


