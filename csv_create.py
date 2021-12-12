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
from pprint import pprint
from typing import List, Tuple
from io import TextIOWrapper
from pymongo.command_cursor import CommandCursor

from db import DBC
from download import ensure_folder
from ciselniky import ORP, Kraje
from invalid_orp import OCKOVANI

class CSVCreatorException(Exception):
    def __init__(self, message: str = ''):
        super().__init__(message)

class CSVCreator():
    OUT_PATH = 'data_csv/'

    def __init__(self) -> None:
        self.dbc = DBC()
        self.kraje = Kraje()
        self.orp = ORP()
    
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

    def write_query_A2_data(self, cursor: CommandCursor, writer) -> int:
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

        quarters = 4
        dates = self.get_quarters_dates(DateParser.parse('2020-10-01'), quarters)
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
        header = ['datum_zacatek', 'datum_konec', 'kraj_nuts_kod', 'kraj_nazev', 'nakazeni_prirustek']
        with self.csv_open('prirustky_kraj') as file:
            writer = self.get_csv_writer(file, header)
            rows = self.write_query_B1_data(cursor, writer)

        expected = (len(Kraje.NUTS3) * quarters)
        if rows != expected:
            raise CSVCreatorException(
                'Loaded invalid amount of rows for query B2 (actual: %i, expected: %i)' % (rows, expected)
            )

    def write_query_B1_data(self, cursor: CommandCursor, writer) -> int:
        count = 0
        nakazeni_zacatek = None
        nuts = None
        zacatek = None
        for doc in cursor:
            if nuts == doc['kraj_nuts_kod']:
                if zacatek is not None:
                    nakazeni_prirustek = doc['kumulativni_pocet_nakazenych'] - nakazeni_zacatek
                    writer.writerow([
                        zacatek,
                        doc['datum'],
                        doc['kraj_nuts_kod'],
                        self.kraje.get_nazev(doc['kraj_nuts_kod']),
                        nakazeni_prirustek
                    ])
                    zacatek = None
                    count += 1
                    continue
            else:
                nuts = doc['kraj_nuts_kod']

            zacatek = doc['datum']
            nakazeni_zacatek = doc['kumulativni_pocet_nakazenych']

        return count

    def query_C1(self) -> None:
        orps = self.map_to_invalid_ORP_codes(self.get_most_populous_ORPs())
        quarters = 4
        dates = self.get_quarters_dates(DateParser.parse('2020-10-01'), quarters)

        count = 0
        header = ['datum_zacatek', 'datum_konec', 'orp_kod', 'orp_nazev', '0-14', '15-59', '60+', 'nakazeni', 'pocet_davek']
        with self.csv_open('orp_ctvrtleti') as file:
            writer = self.get_csv_writer(file, header)
            for orp in orps:
                for i in range(quarters):
                    pos = i * 2
                    infected, vaccinations = self.get_ORP_infected_vaccinations(orp['orp_kod'], dates[pos], dates[pos + 1])
                    writer.writerow([
                        dates[pos],
                        dates[pos + 1],
                        orp['orp_kod'],
                        orp['orp_nazev'],
                        orp['0-14'],
                        orp['15-59'],
                        orp['60+'],
                        infected,
                        vaccinations
                    ])
                    count += 1

        expected = (quarters * 50)
        if count != expected:
            raise CSVCreatorException(
                'Loaded invalid amount of rows for query C1 (actual: %i, expected: %i)' % (count, expected)
            )

    def get_ORP_infected_vaccinations(self, orp_code: int, start: datetime, end: datetime) -> Tuple[int, int]:
        coll = self.dbc.get_collection('nakazeni_orp')
        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'orp_kod': {'$eq': orp_code}},
                        {'datum': {'$gte': start}},
                        {'datum': {'$lte': end}}
                    ]
                }
            },
            {
                '$group': {
                    '_id': None,
                    'nakazeni': {'$sum': '$nove_pripady'}
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        try:
            doc_infected = cursor.next()
        except StopIteration:
            raise CSVCreatorException(
                'Failed to retrieve infection data for ORP (%s) %i %s-%s' % (self.orp.get_orp_nazev(orp_code), orp_code, start, end)
            )

        coll = self.dbc.get_collection('ockovani_orp')
        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'orp_kod': {'$eq': orp_code}},
                        {'datum': {'$gte': start}},
                        {'datum': {'$lte': end}}
                    ]
                }
            },
            {
                '$group': {
                    '_id': None,
                    'pocet_davek': {'$sum': '$pocet_davek'}
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        try:
            doc_vaccinations = cursor.next()
        except StopIteration:
            raise CSVCreatorException(
                'Failed to retrieve vaccination data for ORP (%s) %i %s-%s' % (self.orp.get_orp_kod(orp_code), orp_code, start, end)
            )

        return (doc_infected['nakazeni'], doc_vaccinations['pocet_davek'])

    def get_most_populous_ORPs(self, limit: int = 50) -> List[dict]:
        coll = self.dbc.get_collection('obyvatele_orp')

        pipeline = [
            {
                '$match': {'orp_kod': {'$ne': 1000}} # Praha
            },
            {
                '$project': {
                    'orp_kod': True,
                    'orp_nazev': True,
                    '0-14': True,
                    '15-59': True,
                    '60+': True,
                    'populace': {'$sum': [
                        '$0-14',
                        '$15-59',
                        '$60+'
                    ]}
                }
            },
            {
                '$group': {
                    '_id': '$orp_kod',
                    'orp_nazev': {'$first': '$orp_nazev'},
                    'populace': {'$sum': '$populace'},
                    '0-14': {'$sum': '$0-14'},
                    '15-59': {'$sum': '$15-59'},
                    '60+': {'$sum': '$60+'}
                }
            },
            {
                '$sort': {'populace': -1}
            },
            {
                '$limit': limit
            },
            {
                '$project': {
                    '_id': False,
                    'orp_kod': '$_id',
                    'orp_nazev': True,
                    'populace': True,
                    '0-14': True,
                    '15-59': True,
                    '60+': True
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        orps = list(cursor)

        count = len(orps)
        if count != 50:
            raise CSVCreatorException('Failed to retrieve %i most populous ORPs (retrieved: %i)' % (limit, count))

        return orps

    def query_custom1(self) -> None:
        coll = self.dbc.get_collection('umrti_cr')

        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'casref_od': {'$gte': DateParser.parse('2020-01-01')}},
                        {'vek_kod': {'$eq': ''}}
                    ]
                }
            },
            {
                '$lookup': {
                    'from': 'covid_po_dnech_cr',
                    'let': {
                        'datum_od': '$casref_od',
                        'datum_do': '$casref_do'
                    },
                    'pipeline': [
                        {
                            '$match': {
                                '$expr': {
                                    '$and': [
                                        {'$gte': ['$datum', '$$datum_od']},
                                        {'$lte': ['$datum', '$$datum_do']}
                                    ]
                                }
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'umrti_covid': {'$sum': '$prirustkovy_pocet_umrti'}
                            }
                        }
                    ],
                    'as': 'umrti_covid_arr'
                }
            },
            {
                '$set': {
                    'umrti_covid': {'$first': '$umrti_covid_arr.umrti_covid'}
                }
            },
            {
                '$project': {
                    'datum_od': '$casref_od',
                    'datum_do': '$casref_do',
                    'umrti': '$pocet',
                    'umrti_covid': '$umrti_covid'
                }
            }
        ]
        cursor = coll.aggregate(pipeline)

        header = ['datum_zacatek', 'datum_konec', 'zemreli', 'zemreli_covid']
        with self.csv_open('zemreli_cr') as file:
            writer = self.get_csv_writer(file, header)
            self.write_query_custom1_data(cursor, writer)

    def write_query_custom1_data(self, cursor: CommandCursor, writer) -> int:
        count = 0
        for doc in cursor:
            writer.writerow([
                doc['datum_od'],
                doc['datum_do'],
                doc['umrti'],
                doc.get('umrti_covid', None)
            ])
            count += 1

        return count

    def map_to_invalid_ORP_codes(self, orps: List[dict]) -> List[dict]:
        invalid_orps = dict((x[0], x[1]) for x in OCKOVANI)

        for orp in orps:
            if orp['orp_nazev'] in invalid_orps:
                orp['orp_kod'] = invalid_orps[orp['orp_nazev']]

        return orps

    def get_quarters_dates(self, start: datetime, quarters: int) -> List[datetime]:
        dt = start
        months3 = relativedelta(months=3)
        day = relativedelta(days=1)
        dates = []
        for _ in range(quarters):
            dates.append(dt)
            dt += months3
            dates.append(dt - day)

        return dates

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
        self.query_C1()
        self.query_custom1()

    def log_head(self, cursor, count: int = 2) -> None:
        i = 0
        for doc in cursor:
            #print(doc, end='\n\n')
            pprint(doc)
            print()
            i += 1

            if i > count:
                sys.exit()

if __name__ == '__main__':
    creator = CSVCreator()
    ensure_folder(creator.OUT_PATH)
    creator.create_all_csv_files()
