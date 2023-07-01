from datetime import datetime
from mysql.connector import (connection, errors)

#import for pyinstaller
from mysql.connector.locales.eng import client_error
import mysql.connector.plugins.mysql_native_password


class EuroDB():

    def __init__(self, user, password, host, database) -> None:
        try:
            self.connection = connection.MySQLConnection(
                user=user, password=password, host=host, database=database,
                ssl_disabled=True)
            self.cursor = self.connection.cursor(dictionary=True)
        except errors.Error as err:
            print(f"{err.msg}")
    
    def get_ordini_tagliati(self, machine: str) -> dict:
        result = {}
        q = ("SELECT max(id_avanzamento) as max_id, max(timestamp) as ts, lotto_ordine, numero_ordine "
             "FROM tb_avanzamento ta  WHERE codice_operatore = %s GROUP BY numero_ordine, lotto_ordine")
        self.cursor.execute(q, (machine,))
        for row in self.cursor.fetchall():
            result[f"{row['numero_ordine']}_{row['lotto_ordine']}"] = (row['max_id'], row['ts'])
        return result
    
    def add_log(self, ts: datetime, numero: int, lotto: str, machine: str, workcode: str, fine: bool = True, secondi: int = 1) -> None:
        q = ("INSERT INTO eurodb.tb_avanzamento "
            "(codice_operatore, codice_fase, inizio_fine, numero_ordine, lotto_ordine, timestamp, secondi) "
            "VALUES(%s, %s, %s, %s, %s, %s, %s)")
        self.cursor.execute(q, (machine, workcode, fine, numero, lotto, ts, secondi))

        
if __name__ == '__main__':
    db = EuroDB('marcon', 'ntl721mc', '192.168.29.96', 'eurodb')