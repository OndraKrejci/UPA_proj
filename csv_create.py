##
# @file csv_create.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Retrieve required data from DB and create CSV files

import csv
from datetime import datetime
import sys

from dateutil import parser as DateParser
from dateutil.relativedelta import relativedelta
from io import TextIOWrapper

from typing import List

from db import DBC
from download import ensure_folder
from ciselniky import Kraje

class CSVCreator():
    OUT_PATH = 'data_csv/'

    QUERY_B1_GT_DATE = DateParser.parse('2020-03-31')
    QUERY_B1_LT_DATE = DateParser.parse('2021-04-02')

    CTVRTLETI = [
        (DateParser.parse('2020-04-01'), DateParser.parse('2020-07-01')),
        (DateParser.parse('2020-07-01'), DateParser.parse('2020-10-01')),
        (DateParser.parse('2020-10-01'), DateParser.parse('2021-01-01')),
        (DateParser.parse('2021-01-01'), DateParser.parse('2021-04-01'))
    ]

    def __init__(self) -> None:
        self.dbc = DBC()
        self.kraje = Kraje()
    
    def query_A1(self) -> None:
        coll = self.dbc.get_collection('covid_po_dnech_cr')

        header = ['datum', 'nakazeni', 'vyleceni', 'hospitalizovani', 'testy']
        month = relativedelta(months=1)
        dt = DateParser.parse('2020-04-1')
        dt_now = datetime.now()
        with self.csv_open('covid_po_mesicich') as file:
            writer = self.get_csv_writer(file, header)

            while dt < dt_now:
                doc = coll.find_one({'datum': {'$eq': dt}})
                if doc:
                    writer.writerow([
                        doc['datum'],
                        doc['prirustkovy_pocet_nakazenych'],
                        doc['prirustkovy_pocet_vylecenych'],
                        doc['pacient_prvni_zaznam'],
                        doc['prirustkovy_pocet_provedenych_testu']
                    ])

                dt += month

    def query_A2(self) -> None:
        coll = self.dbc.get_collection('nakazeni_vek_okres_kraj')

        pipeline = [
            {
                '$project': {
                    'kraj_nuts_kod': True,
                    'vek': True,
                    'pohlavi': True
                }
            }
        ]
        cursor = coll.aggregate(pipeline)

        header = ['kraj_nuts_kod', 'kraj_nazev', 'vek', 'pohlavi']
        with self.csv_open('osoby_nakazeni_kraj') as file:
            writer = self.get_csv_writer(file, header)
            self.write_query_A2_data(cursor, writer)

    def write_query_A2_data(self, cursor, writer) -> int:
        count = 0
        for doc in cursor:
            writer.writerow([
                doc['kraj_nuts_kod'],
                self.kraje.get_nazev(doc['kraj_nuts_kod']),
                doc['vek'],
                doc['pohlavi']
            ])
            count += 1

        return count

    def query_B1(self) -> None:
        coll = self.dbc.get_collection('nakazeni_vyleceni_umrti_testy_kraj')

        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'datum': {'$gt': self.QUERY_B1_GT_DATE}},
                        {'datum': {'$lt': self.QUERY_B1_LT_DATE}}
                    ]
                }
            },
            {
                '$project': {
                    'datum': True,
                    'kraj_nuts_kod': True,
                    'kumulativni_pocet_nakazenych': True
                }
            },
            {
                '$sort': {'kraj_nuts_kod': 1, 'datum': 1}
            }
        ]
        cursor = coll.aggregate(pipeline)

        header = ['datum_zacatek', 'datum_konec', 'kraj_nuts_kod', 'kraj_nazev', 'nakazeni']
        with self.csv_open('prirustky_kraj') as file:
            writer = self.get_csv_writer(file, header)
            self.write_query_B1_data(cursor, writer)

    def write_query_B1_data(self, cursor, writer) -> int:
        count = 0
        nakazeni_prirustek = 0
        prirustek_zacatek = 0
        zacatek, konec = self.CTVRTLETI[0]
        for doc in cursor:
            if doc['datum'] == konec:
                nakazeni_prirustek = doc['kumulativni_pocet_nakazenych'] - nakazeni_prirustek
                nakazeni_prirustek = doc['kumulativni_pocet_nakazenych'] - prirustek_zacatek
                writer.writerow([
                    zacatek,
                    konec,
                    doc['kraj_nuts_kod'],
                    self.kraje.get_nazev(doc['kraj_nuts_kod']),
                    nakazeni_prirustek
                ])

                count += 1
                zacatek, konec = self.CTVRTLETI[count % len(self.CTVRTLETI)]

            if doc['datum'] == zacatek:
                nakazeni_prirustek = doc['kumulativni_pocet_nakazenych']
                prirustek_zacatek = doc['kumulativni_pocet_nakazenych']

        return count

    def get_csv_writer(self, file: TextIOWrapper, header: List[str]):
        writer = csv.writer(file, delimiter=';')
        writer.writerow(header)
        return writer

    def csv_open(self, fname: str) -> TextIOWrapper:
        return open('%s%s' % (self.OUT_PATH, fname + '.csv'), 'w', newline='', encoding='UTF-8')

    def create_all_csv_files(self) -> None:
        ensure_folder(self.OUT_PATH)

        self.query_A1()
        self.query_A2()
        self.query_B1()

    def log_head(self, cursor, count: int = 2) -> None:
        i = 0
        for doc in cursor:
            print(doc, end='\n\n')
            i += 1

            if i > count:
                sys.exit()

if __name__ == '__main__':
    creator = CSVCreator()
    #creator.create_all_csv_files()

    creator.query_A1()
