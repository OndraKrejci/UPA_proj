##
# @file csv_create.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Retrieve required data from DB and create CSV files

import csv
import sys

from datetime import datetime
from dateutil import parser as DateParser
from dateutil.relativedelta import relativedelta
from typing import List
from io import TextIOWrapper

from db import DBC
from download import ensure_folder
from ciselniky import Kraje

class CSVCreatorException(Exception):
    def __init__(self, message: str = ''):
        super().__init__(message)

class CSVCreator():
    OUT_PATH = 'data_csv/'

    def __init__(self) -> None:
        self.dbc = DBC()
        self.kraje = Kraje()
    
    def query_A1(self) -> None:
        coll_name = 'covid_po_dnech_cr'
        coll = self.dbc.get_collection(coll_name)

        header = ['zacatek', 'konec', 'nakazeni', 'vyleceni', 'hospitalizovani', 'testy']
        month = relativedelta(months=1)
        day = relativedelta(days=1)
        dt = DateParser.parse('2020-04-1')
        dt_now = datetime.now()
        with self.csv_open('covid_po_mesicich') as file:
            writer = self.get_csv_writer(file, header)

            while dt < dt_now:
                next_dt = dt + month
                month_end = next_dt - day

                pipeline = [
                    {
                        '$match': {
                            '$and': [
                                {'datum': {'$gte': dt}},
                                {'datum': {'$lte': month_end}}
                            ]
                        }
                    },
                    {
                        '$sort': {'datum': 1}
                    },
                    {
                        '$group': {
                            '_id': None,
                            'nakazeni': {'$sum': '$prirustkovy_pocet_nakazenych'},
                            'vyleceni': {'$sum': '$prirustkovy_pocet_vylecenych'},
                            'hospitalizovani': {'$sum': '$pacient_prvni_zaznam'},
                            'testy': {'$sum': '$prirustkovy_pocet_provedenych_testu'},
                        }
                    }
                ]
                cursor = coll.aggregate(pipeline)
                doc = cursor.next()
                if doc:
                    writer.writerow([
                        dt,
                        month_end,
                        doc['nakazeni'],
                        doc['vyleceni'],
                        doc['hospitalizovani'],
                        doc['testy']
                    ])
                else:
                    raise CSVCreatorException(
                        'Failed to retrieve data from collection "%s" for month beginning with %s' % (coll_name, dt)
                    )

                dt = next_dt

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

        pocet_ctvrtleti = 4
        dt = DateParser.parse('2020-04-01')
        months3 = relativedelta(months=3)
        dates = [dt]
        for _ in range(pocet_ctvrtleti):
            dt += months3
            dates.append(dt)

        pipeline = [
            {
                '$match': {
                    'datum': {'$in': dates}
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
            rows = self.write_query_B1_data(cursor, writer)

        expected = (len(Kraje.NUTS3) * pocet_ctvrtleti)
        if rows != expected:
            raise CSVCreatorException(
                'Loaded invalid amount of rows for query B2 (actual: %i, expected: %i)' % (rows, expected)
            )

    def write_query_B1_data(self, cursor, writer) -> int:
        doc = cursor.next()
        count = 0
        prirustek_zacatek = doc['kumulativni_pocet_nakazenych']
        nuts = doc['kraj_nuts_kod']
        zacatek = doc['datum']
        for doc in cursor:
            if nuts == doc['kraj_nuts_kod']:
                nakazeni_prirustek = doc['kumulativni_pocet_nakazenych'] - prirustek_zacatek
                writer.writerow([
                    zacatek,
                    doc['datum'],
                    doc['kraj_nuts_kod'],
                    self.kraje.get_nazev(doc['kraj_nuts_kod']),
                    nakazeni_prirustek
                ])

                count += 1
            else:
                nuts = doc['kraj_nuts_kod']

            zacatek = doc['datum']
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
    ensure_folder(creator.OUT_PATH)
    #creator.create_all_csv_files()

    creator.query_A1()
