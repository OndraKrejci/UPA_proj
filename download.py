
from typing import List

import requests
import os
import sys

DATA_PATH = 'data'

DATA: List[dict] = [
    # KRAJ, OKRES, ORP, OBEC
    # absolutne a prirustkove: nakazeni; nakazeni 65+, nakazeni za tyden a 14 dni
    {'name': 'obce-nakazeni.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/obce.json', 'large': True},

    # KRAJ, OKRES
    # nakaza jednotlivych osob - vek, datum
    {'name': 'kraj-okres-nakazeni.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.json'},
    # vyleceni jednotlivych osob - vek, datum
    {'name': 'kraj-okres-vyleceni.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/vyleceni.json'},
    # umrti jednotlivych osob - vek, datum
    {'name': 'kraj-okres-umrti.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/umrti.json'},
    # kumulativne: nakaza, vyleceni, umrti - datum
    {'name': 'kraj-okres-nakazeni-vyleceni-umrti.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.json'},
    # kumulativne a prirustkove: testy (dohromady, zaznamy pro kraj i okres)
    {'name': 'kraj-okres-testy.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-testy.json'},

    # CZE
    # absolutne a kumulativne: hospitalizace a umrti - datum; detaily k hospitalizaci
    {'name': 'cr-hospitalizace-umrti.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.json'},
    # kumulativne a prirustkove: nakaza, vyleceni, umrti, testy (oddelene AG, PCR) - datum
    {'name': 'cr-nakazeni-vyleceni-umrti-testy.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-vyleceni-umrti-testy.json'},
    # prirustkove: testy (oddelene AG, PCR) - datum; dalsi udaje o testech, pozitivni testy...
    {'name': 'cr-testy.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/testy-pcr-antigenni.json'},

    # ORP
    # absolutne: nakazeni a hospitalizovani (vsichni, 65+, 75+) - den; nakazeni, hospitalizovani, testy - tyden
    {'name': 'orp-nakazeni-hospitalizovani.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/orp.json'},
    # absolutne: ockovani podle bydliste (ORP) a typu vakciny - den
    {'name': 'orp-ockovani-geografie.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani-geografie.json'},

    # KRAJ
    # absolutne: ockovani podle veku a typu vakciny - den
    {'name': 'kraj-ockovani.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/ockovani.json'},

    # CISELNIKY
    {'name': 'vazba-orp-kraj-nuts.csv', 'url': 'https://apl.czso.cz/iSMS/cisexp.jsp?kodcis=65&typdat=1&cisvaz=100_398&datpohl=01.11.2021&cisjaz=203&format=2&separator=%2C'},

    # TODO
    {'name': 'cr-zemreli.csv', 'url': 'https://www.czso.cz/documents/62353418/155512389/130185-21data101921.csv/06c23c55-9c1a-4925-8386-8cd9625787ef?version=1.1'},
    {'name': 'kraj-okres-obyvatelstvo.csv', 'url': 'https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1'},

    # POPULACE
    {'name': 'jmk.xlsx', 'url': 'https://www.czso.cz/documents/11280/17803643/vu06.xlsx/57579f26-b5b5-4512-af06-2c2f27086cae?version=1.21'}
]

def ensure_folder(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def download(url: str, name: str, large: bool = False, rewrite: bool = False, log: bool = True, skipLarge: bool = False) -> None:
    path = '%s/%s' % (DATA_PATH, name)

    if not rewrite and os.path.isfile(path) or large and skipLarge:
        if log:
            print('Skipped:\t%s' % name)
        return

    data = requests.get(url, allow_redirects=True)

    if data.status_code != 200:
        print(data.headers, file=sys.stderr)
        sys.exit()

    file = open(path, 'wb')
    file.write(data.content)
    file.close()

    if log:
        print('Downloaded:\t%s' % name)

def download_data(rewrite: bool = False, log: bool = True, skipLarge = False):
    ensure_folder(DATA_PATH + '/')

    for data in DATA:
        download(data['url'], data['name'], data.get('large', False), rewrite, log, skipLarge)

if __name__ == '__main__':
    download_data()
