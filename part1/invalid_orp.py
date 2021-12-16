##
# @file invalid_orp.py
# @author Ondřej Krejčí xkrejc69@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Invalid ORP code detection

import json
import sys
import csv

from typing import Union

from .download import DATA_PATH
from .ciselniky import ORP

class InvalidORPCodeDetector():
    FILES = {
        'obce-nakazeni.json': {
            'kod': 'orp_kod',
            'nazev': 'orp_nazev'
        },
        'orp-ockovani-geografie.json': {
            'kod': 'orp_bydliste_kod',
            'nazev': 'orp_bydliste'
        },
        'orp-nakazeni-hospitalizovani.json': {
            'kod': 'orp_kod',
            'nazev': 'orp_nazev'
        }
    }

    def __init__(self, orp: Union[ORP, None] = None) -> None:
        if orp is None:
            self.orp_helper = ORP()
        else:
            self.orp_helper = orp

    def get_invalid_orp_set(self, fname: str, code_key: Union[str, None] = None, name_key: Union[str, None] = None) -> set:
        if code_key is None or name_key is None:
            if fname in self.FILES:
                code_key = self.FILES[fname]['kod']
                name_key = self.FILES[fname]['nazev']
            else:
                print('Unknown file name %s' % fname, file=sys.stderr)
                sys.exit(3)

        with open('%s/%s' % (DATA_PATH, fname), 'r', encoding='utf-8') as file:
            doc = json.load(file)['data']

        missing_orps = set()
        for data in doc:
            if data[code_key] not in self.orp_helper.orp.keys():
                missing_orps.add((data[name_key], data[code_key]))

        return missing_orps

    @staticmethod
    def invalid_orp_set_to_dict(orp_set: set) -> dict:
        return dict((x[0], x[1]) for x in orp_set)

    def save_invalid_orp_codes(self, fname: str, out_fname: str) -> None:
        missing_orps = self.get_invalid_orp_set(fname)
        with open(out_fname, 'w', encoding='utf-8', newline='') as out_file:
            writer = csv.writer(out_file, delimiter=';')
            writer.writerow(['orp_code', 'orp_name'])
            for orp_name, orp_code in missing_orps:
                writer.writerow([orp_code, orp_name])

    def load_invalid_orp_dict(self, fname: str) -> dict:
        missing_orps = {}
        with open(fname, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file, delimiter=';')
            for line in reader:
                missing_orps[line['orp_name']] = line['orp_code']

        return missing_orps

if __name__ == '__main__':
    detector = InvalidORPCodeDetector()
    detector.save_invalid_orp_codes('orp-ockovani-geografie.json', '%s/%s' % (DATA_PATH, 'ockovani_invalid_orp.csv'))

    #ORP_NAKAZENI = detector.get_invalid_orp_set('obce-nakazeni.json')
    #ORP_OCKOVANI = detector.get_invalid_orp_set('orp-ockovani-geografie.json')
    #ORP_HOSPITALIZOVANI = detector.get_invalid_orp_set('orp-nakazeni-hospitalizovani.json')
