##
# @file ciselniky.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 11/2021


import csv

from typing import Tuple, Union

from .download import DATA_PATH

UZEMI_OKRES = 101
UZEMI_KRAJ = 100
UZEMI_REPUBLIKA = 97

OBYVATELSTVO_KRAJ_CSU7700 = {
    '0-4':   '400000600005000',
    '5-9':   '400005610010000',
    '10-14': '410010610015000',
    '15-19': '410015610020000',
    '20-24': '410020610025000',
    '24-29': '410025610030000',
    '30-34': '410030610035000',
    '35-39': '410035610040000',
    '40-44': '410040610045000',
    '45-49': '410045610050000',
    '50-54': '410050610055000',
    '55-59': '410055610060000',
    '60-64': '410060610065000',
    '65-69': '410065610070000',
    '70-74': '410070610075000',
    '75-79': '410075610080000',
    '80-84': '410080610085000',
    '85-89': '410085610090000',
    '90-94': '410090610095000',
    '95+':   '410095799999000'
}

class ORP:
    def __init__(self) -> None:
        self.nazvy, self.zkracene, self.orp = ORP.get_orp_dicts()

    @staticmethod
    def load_orp_zkracene() -> dict:
        with open('%s/%s' % (DATA_PATH, 'orp-ciselnik.csv'), 'r', encoding='windows-1250') as file:
            header = [
                'KODJAZ',
                'AKRCIS',
                'KODCIS',
                'orp_kod',
                'orp_zkraceny_nazev',
                'orp_nazev',
                'ADMPLOD',
                'ADMNEPO',
                'KOD_RUIAN'
            ]
            reader = csv.DictReader(file, header)
            reader.__next__() # header

            orp_data = {}
            for line in reader:
                orp_data[int(line['orp_kod'])] = line['orp_zkraceny_nazev']

            return orp_data

    @staticmethod
    def create_orp_kraje_vazba_json() -> dict:
        orp_zkracene = ORP.load_orp_zkracene()
        with open('%s/%s' % (DATA_PATH, 'vazba-orp-kraj.csv'), 'r', encoding='windows-1250') as file:
            header = [
                'KODJAZ',
                'TYPVAZ',
                'AKRCIS1',
                'KODCIS1',
                'orp_kod',
                'orp_nazev',
                'AKRCIS2',
                'KODCIS2',
                'kraj_kod',
                'kraj_nazev'
            ]
            reader = csv.DictReader(file, header)
            reader.__next__() # header

            orp_data = {}
            for line in reader:
                orp_kod = int(line['orp_kod'])
                kraj_kod = int(line['kraj_kod'])
                orp_data[orp_kod] = {
                    'nazev': line['orp_nazev'],
                    'zkraceny_nazev': orp_zkracene[orp_kod],
                    'kraj_kod': kraj_kod,
                    'kraj_nazev': line['kraj_nazev']
                }

            return orp_data

    @staticmethod
    def get_orp_dicts() -> Tuple[dict, dict, dict]:
        orp_kraj_vazba = ORP.create_orp_kraje_vazba_json()

        nazvy = {}
        zkracene = {}
        for orp_kod, data in orp_kraj_vazba.items():
            nazvy[data['nazev']] = orp_kod
            zkracene[data['zkraceny_nazev']] = orp_kod

        return (nazvy, zkracene, orp_kraj_vazba)

    def get_orp_kod(self, nazev: str) -> Union[int, None]:
        if nazev in self.nazvy:
            return self.nazvy[nazev]
        elif nazev in self.zkracene:
            return self.zkracene[nazev]

        return None

    def get_orp_nazev(self, kod: int) -> Union[str, None]:
        if kod in self.orp:
            return self.orp[kod]['nazev']

        return None

    def get_kraj_kod(self, kod: int) -> Union[int, None]:
        if kod in self.orp:
            return self.orp[kod]['kraj_kod']

        return None

class Kraje:
    def __init__(self) -> None:
        self.kody = Kraje.load_kraje_ciselnik()

    NUTS3 = {
        'CZ010': 'Hlavní město Praha',
        'CZ020': 'Středočeský kraj',
        'CZ031': 'Jihočeský kraj',
        'CZ032': 'Plzeňský kraj',
        'CZ041': 'Karlovarský kraj',
        'CZ042': 'Ústecký kraj',
        'CZ051': 'Liberecký kraj',
        'CZ052': 'Královéhradecký kraj',
        'CZ053': 'Pardubický kraj',
        'CZ063': 'Kraj Vysočina',
        'CZ064': 'Jihomoravský kraj',
        'CZ071': 'Olomoucký kraj',
        'CZ072': 'Zlínský kraj',
        'CZ080': 'Moravskoslezský kraj'
    }

    @staticmethod
    def load_kraje_ciselnik() -> dict:
        with open('%s/%s' % (DATA_PATH, 'kraj-ciselnik.csv'), 'r', encoding='windows-1250') as file:
            header = [
                'KODJAZ',
                'AKRCIS',
                'KODCIS',
                'kraj_kod',
                'ZKRTEXT',
                'kraj_text',
                'ADMPLOD',
                'ADMNEPO',
                'kraj_nuts',
                'KOD_RUIAN',
                'ZKRKRAJ'
            ]
            reader = csv.DictReader(file, header)
            reader.__next__() # header

            kody = {}
            for line in reader:
                kod = int(line['kraj_kod'])
                kody[kod] = {
                    'nazev': line['kraj_text'],
                    'nuts': line['kraj_nuts']
                }

            return kody

    def get_nuts(self, kod: int) -> Union[str, None]:
        if kod in self.kody:
            return self.kody[kod]['nuts']

        return None

    @staticmethod
    def get_nazev(nuts: str) -> Union[str, None]:
        return Kraje.NUTS3.get(nuts, None)

def get_csu7700_ciselnik() -> dict:
    with open('%s/%s' % (DATA_PATH, 'csu7700.csv'), 'r', encoding='windows-1250') as file:
        reader = csv.DictReader(file)
        vek_kody = {}
        for line in reader:
            if (
                (
                    line['MIN_OSTRY'] is not None and line['MAX_TUPY'] is not None
                    and line['MIN_OSTRY'].isnumeric() and line['MAX_TUPY'].isnumeric()
                    and int(line['MIN_OSTRY']) < 100
                    and ((int(line['MIN_OSTRY']) + 1) == int(line['MAX_TUPY']))
                ) or line['CHODNOTA'] == '420100799999000'
            ):
                vek_kody[int(line['MIN_OSTRY'])] = line['CHODNOTA']

        return vek_kody
