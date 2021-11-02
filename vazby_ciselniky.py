
import csv

from typing import Tuple

from download import DATA_PATH

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

def create_orp_kraje_vazba_json() -> dict:
    orp_zkracene = load_orp_zkracene()
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

        orp_data = {}
        first = True
        for line in reader:
            if first:
                first = False
                continue

            orp_kod = int(line['orp_kod'])
            orp_data[orp_kod] = {
                'nazev': line['orp_nazev'],
                'zkraceny_nazev': orp_zkracene[orp_kod],
                'kraj_kod': line['kraj_kod'],
                'kraj_nazev': line['kraj_nazev']
            }

        return orp_data

def get_orp_nazvy() -> Tuple[dict, dict]:
    orp_kraj_vazba = create_orp_kraje_vazba_json()

    nazvy = {}
    zkracene = {}
    for orp_kod, data in orp_kraj_vazba.items():
        nazvy[orp_kod] = data['nazev']
        zkracene[orp_kod] = data['zkraceny_nazev']

    return (nazvy, zkracene)


if __name__ == '__main__':
    pass
