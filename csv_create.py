##
# @file csv_create.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Retrieve required data from DB and create CSV files

import csv
import sys
import argparse

from dateutil import parser as DateParser
from dateutil.relativedelta import relativedelta
from pprint import pprint
from typing import Dict, List, Tuple, Union
from io import TextIOWrapper
from pymongo.command_cursor import CommandCursor
from datetime import datetime

from part1.db import DBC
from part1.download import ensure_folder
from part1.ciselniky import ORP, Kraje, OBYVATELSTVO_KRAJ_CSU7700
from part1.invalid_orp import InvalidORPCodeDetector

class CSVCreatorException(Exception):
    def __init__(self, message: str = ''):
        super().__init__(message)

class CSVCreator():
    OUT_PATH = 'data_csv/'

    COMPATIBILITY_VERSION = '3.4.24'
    NEW_VERSION = '5'

    FIRST_QUARTER_DATE = DateParser.parse('2020-10-01')

    def __init__(self, compatibility: bool = False, log: bool = True, dbc: Union[DBC, None] = None) -> None:
        if dbc is None:
            self.dbc = DBC()
        else:
            self.dbc = dbc

        self.kraje = Kraje()
        self.orp = ORP()

        self.invalid_orp_helper = InvalidORPCodeDetector(self.orp)
        self.invalid_orp_set = self.invalid_orp_helper.get_invalid_orp_set('orp-ockovani-geografie.json')
        self.invalid_orp_dict = self.invalid_orp_helper.invalid_orp_set_to_dict(self.invalid_orp_set)

        self.compatibility = compatibility
        self.log = log

    def use_new_version(self) -> bool:
        return (not self.compatibility and self.dbc.check_version(self.NEW_VERSION))
    
    def query_A1(self) -> None:
        csv_name = 'A1-covid_po_mesicich'
        self.log_csv(csv_name)

        coll_name = 'covid_po_dnech_cr'
        coll = self.dbc.get_collection(coll_name)

        header = ['zacatek', 'konec', 'nakazeni', 'vyleceni', 'hospitalizovani', 'testy']
        month = relativedelta(months=1)
        day = relativedelta(days=1)
        dt = DateParser.parse('2020-04-01')
        dt_end = DateParser.parse('2021-12-01')
        with self.csv_open(csv_name) as file:
            writer = self.get_csv_writer(file, header)

            while dt < dt_end:
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
        csv_name = 'A2-osoby_nakazeni_kraj'
        self.log_csv(csv_name)

        coll = self.dbc.get_collection('nakazeni_vek_okres_kraj')

        pipeline = [
            {
                '$project': {
                    'kraj_nuts_kod': True,
                    'vek': True,
                    'nakaza_v_zahranici': True
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        header = ['kraj_nuts_kod', 'kraj_nazev', 'vek']
        with self.csv_open(csv_name) as file:
            writer = self.get_csv_writer(file, header)
            self.write_query_A2_data(cursor, writer)

    def write_query_A2_data(self, cursor: CommandCursor, writer) -> int:
        count = 0
        for doc in cursor:
            kraj_nuts_kod = doc['kraj_nuts_kod']

            if kraj_nuts_kod is None:
                if doc['nakaza_v_zahranici']:
                    kraj_nuts_kod = 'zahranici'
                    kraj_nazev = 'zahraničí'
                else:
                    kraj_nuts_kod = 'neznamy'
                    kraj_nazev = 'neznámý'
            else:
                kraj_nazev = self.kraje.get_nazev(doc['kraj_nuts_kod'])
            
            writer.writerow([
                kraj_nuts_kod,
                kraj_nazev,
                doc['vek']
            ])
            count += 1

        return count

    def query_B1(self) -> None:
        csv_name = 'B1-nakazeni_kumulativne_kraj'
        self.log_csv(csv_name)

        coll = self.dbc.get_collection('nakazeni_vyleceni_umrti_testy_kraj')

        total_quarters = 4
        dates = self.get_quarters_dates(self.FIRST_QUARTER_DATE, total_quarters)
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
        header = ['datum', 'kraj_nuts_kod', 'kraj_nazev', 'kraj_populace', 'kumulativni_pocet_nakazenych']
        with self.csv_open(csv_name) as file:
            writer = self.get_csv_writer(file, header)
            rows = self.write_query_B1_data(cursor, writer)

        expected = (len(Kraje.NUTS3) * total_quarters * 2) # 2 = start and end of quarter
        if rows != expected:
            raise CSVCreatorException(
                'Loaded invalid amount of rows for query B2 (actual: %i, expected: %i)' % (rows, expected)
            )

    def get_population_collection_max_date(self) -> int:
        coll_name = 'obyvatelstvo_kraj'
        coll = self.dbc.get_collection(coll_name)

        pipeline = [
            {
                '$group': {
                    '_id': None,
                    'max_datum': {'$max': '$casref_do'}
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        try:
            doc = cursor.next()
        except StopIteration:
            raise CSVCreatorException('Failed to load latest date from collection %s' % coll_name)
        return doc['max_datum']

    def get_regions_population_total(self) -> Dict[str, int]:
        coll = self.dbc.get_collection('obyvatelstvo_kraj')

        max_datum = self.get_population_collection_max_date()

        pipeline = [
            {
                '$match': {
                    '$and': [
                        {'pohlavi_kod': {'$eq': ''}},
                        {'vek_kod': {'$eq': ''}},
                        {'casref_do': {'$eq': max_datum}}
                    ]
                }
            },
            {
                '$project': {
                    'kraj_nuts_kod': '$nuts_kod',
                    'populace': '$pocet'
                }
            }
        ]
        cursor = coll.aggregate(pipeline)
        regions_population = list(cursor)

        count = len(regions_population)
        expected = len(Kraje.NUTS3)
        if count != expected:
            raise CSVCreatorException('Failed to retrieve population of all regions (expected: %i, retrieved: %i)' % (expected, count))

        regions_population = {x['kraj_nuts_kod']: x['populace'] for x in regions_population}
        return regions_population

    def write_query_B1_data(self, cursor: CommandCursor, writer) -> int:
        count = 0
        regions_population = self.get_regions_population_total()
        for doc in cursor:
            writer.writerow([
                doc['datum'],
                doc['kraj_nuts_kod'],
                self.kraje.get_nazev(doc['kraj_nuts_kod']),
                regions_population[doc['kraj_nuts_kod']],
                doc['kumulativni_pocet_nakazenych']
            ])

            count += 1

        return count

    def query_C1(self) -> None:
        csv_name = 'C1-orp_ctvrtleti'
        self.log_csv(csv_name)

        orps = self.map_to_invalid_ORP_codes(self.get_most_populous_ORPs())
        total_quarters = 4
        dates = self.get_quarters_dates(self.FIRST_QUARTER_DATE, total_quarters)

        count = 0
        header = ['datum_zacatek', 'datum_konec', 'orp_kod', 'mzcr_orp_kod', 'orp_nazev', '0-14', '15-59', '60+', 'nakazeni', 'pocet_davek']
        with self.csv_open(csv_name) as file:
            writer = self.get_csv_writer(file, header)
            for orp in orps:
                for i in range(total_quarters):
                    pos = i * 2
                    infected, vaccinations = self.get_ORP_infected_and_vaccinations(orp['orp_kod'], dates[pos], dates[pos + 1])
                    writer.writerow([
                        dates[pos],
                        dates[pos + 1],
                        self.orp.get_orp_kod(orp['orp_nazev']), # code according to the ČSÚ ORP codebook
                        orp['orp_kod'],
                        orp['orp_nazev'],
                        orp['0-14'],
                        orp['15-59'],
                        orp['60+'],
                        infected,
                        vaccinations
                    ])
                    count += 1

        expected = (total_quarters * 50)
        if count != expected:
            raise CSVCreatorException(
                'Loaded invalid amount of rows for query C1 (actual: %i, expected: %i)' % (count, expected)
            )

    def get_ORP_infected_and_vaccinations(self, orp_code: int, start: datetime, end: datetime) -> Tuple[int, int]:
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

    def query_D1(self) -> None:
        csv_name = 'D1-zemreli_cr'
        self.log_csv(csv_name)

        header = ['datum_zacatek', 'datum_konec', 'zemreli', 'zemreli_covid']

        coll = self.dbc.get_collection('umrti_cr')

        if self.use_new_version():
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
                            },
                            {
                                '$limit': 1
                            }
                        ],
                        'as': 'umrti_covid_arr'
                    }
                },
                {
                    '$unwind': {
                        'path': '$umrti_covid_arr',
                        'preserveNullAndEmptyArrays': True
                    }
                },
                {
                    '$project': {
                        'datum_od': '$casref_od',
                        'datum_do': '$casref_do',
                        'umrti': '$pocet',
                        'umrti_covid': '$umrti_covid_arr.umrti_covid'
                    }
                }
            ]
            cursor = coll.aggregate(pipeline)

            with self.csv_open(csv_name) as file:
                writer = self.get_csv_writer(file, header)
                self.write_query_D1_data(cursor, writer)
        else:
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
                    '$project': {
                        'datum_od': '$casref_od',
                        'datum_do': '$casref_do',
                        'umrti': '$pocet'
                    }
                }
            ]
            cursor = coll.aggregate(pipeline)

            coll_covid = self.dbc.get_collection('covid_po_dnech_cr')

            with self.csv_open(csv_name) as file:
                writer = self.get_csv_writer(file, header)
                for doc in cursor:
                    pipeline_covid = [
                        {
                            '$match': {
                                '$and': [
                                    {'datum': {'$gte': doc['datum_od']}},
                                    {'datum': {'$lte': doc['datum_do']}}
                                ]
                            }
                        },
                        {
                            '$group': {
                                '_id': None,
                                'umrti_covid': {'$sum': '$prirustkovy_pocet_umrti'}
                            }
                        },
                        {
                            '$limit': 1
                        }
                    ]
                    cursor_covid = coll_covid.aggregate(pipeline_covid)
                    try:
                        doc_umrti_covid = cursor_covid.next()
                        doc['umrti_covid'] = doc_umrti_covid['umrti_covid']
                    except StopIteration:
                        doc['umrti_covid'] = None
                        
                    self.write_query_D1_row(doc, writer)

    def write_query_D1_row(self, doc: dict, writer) -> None:
        writer.writerow([
            doc['datum_od'],
            doc['datum_do'],
            doc['umrti'],
            doc.get('umrti_covid', None)
        ])

    def write_query_D1_data(self, cursor: CommandCursor, writer) -> int:
        count = 0
        for doc in cursor:
            self.write_query_D1_row(doc, writer)
            count += 1
        return count

    def query_D2(self) -> None:
        csv_name = 'D2-zemreli_vekove_kategorie'
        self.log_csv(csv_name)

        coll = self.dbc.get_collection('umrti_vek_okres_kraj')

        population_groups = self.get_cze_population_groups()

        header = ['vekova_kategorie', 'pocet_obyvatel', 'umrti_covid']
        with self.csv_open(csv_name) as file:
            writer = self.get_csv_writer(file, header)
            for group_name, population in population_groups.items():
                if group_name != '90+':
                    start, end = (int(age) for age in group_name.split('-'))
                    and_cond = [
                        {'vek': {'$gte': start}},
                        {'vek': {'$lte': end}}
                    ]
                else:
                    and_cond = [
                        {'vek': {'$gte': 90}}
                    ]

                pipeline = [
                    {
                        '$match': {
                            '$and': and_cond
                        }
                    },
                    {
                        '$count': 'umrti'
                    }
                ]
                cursor = coll.aggregate(pipeline)
                try:
                    doc_count = cursor.next()
                    umrti_covid = doc_count['umrti']
                except StopIteration:
                    umrti_covid = 0
                
                doc = {
                    'vekova_kategorie': group_name,
                    'pocet_obyvatel': population,
                    'umrti_covid': umrti_covid
                }
                self.write_query_D2_row(doc, writer)

    def write_query_D2_row(self, doc: dict, writer) -> None:
        writer.writerow([
            doc['vekova_kategorie'],
            doc['pocet_obyvatel'],
            doc['umrti_covid']
        ])

    def get_cze_population_groups(self) -> Dict[str, int]:
        coll = self.dbc.get_collection('obyvatelstvo_kraj')

        max_datum = self.get_population_collection_max_date()

        age_groups = {}

        i = 0
        while i <= 90:
            if i < 90:
                keys = ['%i-%i' % (i, (i + 4)), '%i-%i' % ((i + 5), (i + 9))]
                age_group = '%i-%i' % (i, (i + 9))
            else:
                keys = ['90-94', '95+']
                age_group = '90+'

            age_codes = [OBYVATELSTVO_KRAJ_CSU7700[key] for key in keys]

            pipeline = [
                {
                    '$match': {
                        '$and': [
                            {'pohlavi_kod': {'$eq': ''}},
                            {'vek_kod': {'$in': age_codes}},
                            {'casref_do': {'$eq': max_datum}}
                        ]
                    }
                },
                {
                    '$group': {
                        '_id': None,
                        'pocet': {'$sum': '$pocet'}
                    }
                }
            ]
            cursor = coll.aggregate(pipeline)
            try:
                doc = cursor.next()
                age_groups[age_group] = doc['pocet']
            except StopIteration:
                raise CSVCreatorException('Failed to retrieve population for age groups %s, %s'% (keys[0], keys[1]))

            i += 10

        return age_groups

    def map_to_invalid_ORP_codes(self, orps: List[dict]) -> List[dict]:
        for orp in orps:
            if orp['orp_nazev'] in self.invalid_orp_dict:
                orp['orp_kod'] = self.invalid_orp_dict[orp['orp_nazev']]

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
        if self.log:
            version = self.NEW_VERSION if self.use_new_version() else self.COMPATIBILITY_VERSION
            print('Using queries for MongoDB version %s\n' % version)

        ensure_folder(self.OUT_PATH)

        self.query_A1()
        self.query_A2()
        self.query_B1()
        self.query_C1()
        self.query_D1()
        self.query_D2()

    def log_csv(self, csv_name: str) -> None:
        if self.log:
            print('Creating %s' % csv_name + '.csv', flush=True)

    def log_head(self, cursor, count: int = 5) -> None:
        i = 0
        for doc in cursor:
            pprint(doc)
            print()

            i += 1
            if i > count:
                break
        sys.exit()

def main() -> None:
    argparser = argparse.ArgumentParser()
    argparser.add_argument('-H', '--host', dest='host', help='DB server host', default=DBC.DEFAULT_HOST, type=str, required=False)
    argparser.add_argument('-p', '--port', dest='port', help='DB server port number', default=DBC.DEFAULT_PORT, type=int, required=False)
    argparser.add_argument('-t', '--timeout', dest='timeout', help='timeout for server connection', default=None, type=int, required=False)

    args, _ = argparser.parse_known_args(sys.argv)

    dbc = DBC(args.host, args.port, args.timeout)
    csv_creator = CSVCreator(compatibility=True, log=True, dbc=dbc)

    try:
        csv_creator.create_all_csv_files()
    except CSVCreatorException as exc:
        print(exc)
        sys.exit(2)

if __name__ == '__main__':
    creator = CSVCreator(compatibility=True)
    ensure_folder(creator.OUT_PATH)
    creator.create_all_csv_files()

    #creator.query_D1()
