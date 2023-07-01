"""
Read DbStatistics.sqlite
and write avanzamento record on euroDB

INSERT INTO eurodb.tb_avanzamento
(codice_operatore, codice_fase, inizio_fine, numero_ordine, lotto_ordine, `timestamp`, secondi, bilancelle, carrello, multiordine)
VALUES('CNC01', 'A1', b'0', 0, '', CURRENT_TIMESTAMP, 0, 0.0, '', 1);

"""

import os
import configparser
import glob
from datetime import datetime
import re

from db import EuroDB
from dbStats import StatsDB

script_dir = os.getcwd()
config = configparser.ConfigParser()
config.read(os.path.join(script_dir, "config.ini"))

logs_registrati = {}

statsdb = StatsDB(config["sqlite"]["path"])
eurodb = EuroDB(
    config["mysql"]["user"],
    config["mysql"]["password"],
    config["mysql"]["host"],
    config["mysql"]["database"],
)


VERBOSE = config["info"]["verbose"]


def verbose(s: str) -> None:
    if VERBOSE:
        print(s)

def is_number_valid(numero_ordine: str) -> bool:
    if re.match('\d{6}_[0-9A-Z]', numero_ordine):
        return True
    return False

def get_numero(numero_ordine: str) -> str:
    return numero_ordine[:6]

def get_lotto(numero_ordine: str) -> str:
    return numero_ordine[-1]

def is_updatable(new_date: datetime, rec_date: datetime) -> bool:
    time_span = new_date - rec_date
    verbose(f"From {rec_date} to {new_date} sono trascorsi {time_span.days} giorni")
    if time_span.days > 0:
        return True
    return False

def main() -> None:
    c = 0
    ordini_tagliati = statsdb.get_ordini_tagliati()
    ordini_registrati = eurodb.get_ordini_tagliati()

    for ordine, tempi in ordini_tagliati.items():
        if not(is_number_valid(ordine)):
            # verbose(f"{ordine:<30} NON è un numero valido")
            continue
        if ordine in ordini_registrati.keys():
            verbose(f"{ordine} già registrato il {ordini_registrati[ordine][1]}")
            if is_updatable(datetime.strptime(tempi[1], '%Y-%m-%d %H:%M:%S'), ordini_registrati[ordine][1]):
                verbose(f"{ordine} aggiornato al nuovo giorno.")
                eurodb.add_log(tempi[1], get_numero(ordine), get_lotto(ordine), True, 1)
            continue
        verbose(f"Registro inizio e fine ordine {ordine}")
        eurodb.add_log(tempi[0], get_numero(ordine), get_lotto(ordine), False, 0)
        c += 1
        eurodb.add_log(tempi[1], get_numero(ordine), get_lotto(ordine), True, 1)
        c += 1

    eurodb.connection.commit()
    eurodb.connection.close()
    statsdb.connection.close()
    verbose(f"Aggiunto {c} nuove registrazioni.")

if __name__ == "__main__":
    main()
