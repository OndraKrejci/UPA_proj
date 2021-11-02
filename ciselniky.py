
import csv

from typing import Tuple, Union

from download import DATA_PATH

CSU7700 = {
    '400000600005000': '0-4 ',
    '400005610010000': '5-9',
    '410010610015000': '10-14',
    '410015610020000': '15-19',
    '410020610025000': '20-24',
    '410025610030000': '24-29',
    '410030610035000': '30-34',
    '410035610040000': '35-39',
    '410040610045000': '40-44',
    '410045610050000': '45-49',
    '410050610055000': '50-54',
    '410055610060000': '55-59',
    '410060610065000': '60-64',
    '410065610070000': '65-69',
    '410070610075000': '70-74',
    '410075610080000': '75-79',
    '410080610085000': '80-84',
    '410085610090000': '85-89',
    '410090610095000': '90-94',
    '410095799999000': '95 a více'
}

UZEMI_OKRES = 101
UZEMI_KRAJ = 100
UZEMI_REPUBLIKA = 97

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

class ORP:
    def __init__(self) -> None:
        self.nazvy, self.zkracene = ORP.get_orp_nazvy()

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
        with open('%s/%s' % (DATA_PATH, 'vazba-orp-kraj-nuts.csv'), 'r', encoding='windows-1250') as file:
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
                orp_data[orp_kod] = {
                    'nazev': line['orp_nazev'],
                    'zkraceny_nazev': orp_zkracene[orp_kod],
                    'kraj_kod': line['kraj_kod'],
                    'kraj_nazev': line['kraj_nazev']
                }

            return orp_data

    @staticmethod
    def get_orp_nazvy() -> Tuple[dict, dict]:
        orp_kraj_vazba = ORP.create_orp_kraje_vazba_json()

        nazvy = {}
        zkracene = {}
        for orp_kod, data in orp_kraj_vazba.items():
            nazvy[data['nazev']] = orp_kod
            zkracene[data['zkraceny_nazev']] = orp_kod

        return (nazvy, zkracene)

    def get_orp_kod(self, nazev: str) -> Union[int, None]:
        if nazev in self.nazvy:
            return self.nazvy[nazev]
        elif nazev in self.zkracene:
            return self.zkracene[nazev]

        return None

