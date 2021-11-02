
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
    {'name': 'orp-ciselnik.csv', 'url': 'https://apl.czso.cz/iSMS/cisexp.jsp?kodcis=65&typdat=0&cisvaz=80007_97&datpohl=02.11.2021&cisjaz=203&format=2&separator=%2C'},
    {'name': 'kraj-ciselnik.csv', 'url': 'https://apl.czso.cz/iSMS/cisexp.jsp?kodcis=100&typdat=0&cisvaz=80007_885&datpohl=02.11.2021&cisjaz=203&format=2&separator=%2C'},

    # TODO
    {'name': 'cr-zemreli.csv', 'url': 'https://www.czso.cz/documents/62353418/155512389/130185-21data101921.csv/06c23c55-9c1a-4925-8386-8cd9625787ef?version=1.1'},
    {'name': 'kraj-okres-obyvatelstvo.csv', 'url': 'https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1'},

    # POPULACE
    {'name': 'praha.xlsx', 'url': 'https://www.czso.cz/documents/10180/143060149/33012121q1s06.xlsx/12ed34b5-29cb-4415-87cb-f2841aaf50fc?version=1.3'}, # https://www.czso.cz/csu/czso/s-vybrane-udaje-podle-nizsich-uzemne-spravnich-celku-kraje-hl-m-praha-d8fn9ncq8e
    {'name': 'stredocesky.xlsx', 'url': 'https://www.czso.cz/documents/11240/162915457/20_petilete_ORP.xlsx/7637f17e-3edf-4089-baba-666a2df71d96?version=1.1'}, # https://www.czso.cz/csu/xs/vekove_slozeni_obyvatelstva_stc_kraje
    {'name': 'jihocesky.xlsx', 'url': 'https://www.czso.cz/documents/11256/35330526/r6.xlsx/7eb9d831-d423-4cb6-b971-9cc17c8880d9?version=1.2'}, # https://www.czso.cz/csu/xc/vybrane_udaje_za_vsechny_so_orp
    {'name': 'plzensky.xlsx', 'url': 'https://www.czso.cz/documents/10180/142908552/33010921q1r6.xlsx/3a4ca1fb-c7cc-4e19-a305-7533b42c4edf?version=1.1'}, # https://www.czso.cz/csu/czso/r-spravni-obvody-obci-s-rozsirenou-pusobnosti-7sjgutioxw
    {'name': 'karlovarsky.xlsx', 'url': 'https://www.czso.cz/documents/11244/163423587/33008421q1r6.xlsx/78201c7a-6e75-474d-b847-010869eca3b2?version=1.3'}, # https://www.czso.cz/csu/xk/pocet-obyvatel-podle-petiletych-vekovych-skupin-k-31-12-2020-tab-r6
    {'name': 'ustecky.xlsx', 'url': 'https://www.czso.cz/documents/10180/141845530/33008621q1r6.xlsx/4269d86e-fe4e-48c8-b8b6-91891aad506c?version=1.2'}, # https://www.czso.cz/csu/xu/vybrane-udaje-za-vsechny-spravni-obvody-obci-s-rozsirenou-pusobnosti-so-orp
    {'name': 'liberecky.xlsx', 'url': 'https://www.czso.cz/documents/10180/142381673/33008821q1r6.xlsx/68e996a9-6ab9-4a25-be96-4ce0de3f2cee?version=1.0'}, # https://www.czso.cz/csu/czso/2021q1-r-vybrane-udaje-o-spravnich-obvodech-obci-s-rozsirenou-pusobnosti-libereckeho-kraje
    {'name': 'kralovehradecky.xlsx', 'url': 'https://www.czso.cz/documents/11264/17847695/33009020q1r6.xlsx/1e9cce71-eece-4467-8cda-33df86558716?version=1.1'}, # https://www.czso.cz/csu/xh/vybrane_udaje_o_spravnich_obvodech_orp
    {'name': 'pardubicky.xlsx', 'url': 'https://www.czso.cz/documents/10180/142303940/33009321q1r6.xlsx/62591f85-85ad-4ada-8c2f-637674575d14?version=1.1'}, # https://www.czso.cz/csu/czso/r-spravni-obvody-obci-s-rozsirenou-pusobnosti-so-orp-pardubickeho-kraje-mfx4ug0p08
    {'name': 'vysocina.xlsx', 'url': 'https://www.czso.cz/documents/11268/17848141/ORP6.xlsx/90579218-9cae-45e1-a0cf-f992364be07b?version=1.12'}, # https://www.czso.cz/csu/xj/udaje_o_obcich_s_rozsirenou_pusobnosti_za_rok_2013
    {'name': 'jihomoravsky.xlsx', 'url': 'https://www.czso.cz/documents/11280/17803643/vu06.xlsx/57579f26-b5b5-4512-af06-2c2f27086cae?version=1.21'}, # https://www.czso.cz/csu/xb/vybrane_udaje_za_spravni_obvody_orp_
    {'name': 'oloumocky.xlsx', 'url': 'https://www.czso.cz/documents/10180/141885135/33009721q1r06.xlsx/47b4ad4d-29e7-416e-964e-e7b34f170425?version=1.1'}, # https://www.czso.cz/csu/xm/vybrane-udaje-o-so-orp-v-roce-2020
    {'name': 'moravskoslezsky.xlsx', 'url': 'https://www.czso.cz/documents/10180/142044384/33010121q1r06.xlsx/91ec1d18-64c8-4d5f-8df7-94d0c3c4110c?version=1.0'}, # https://www.czso.cz/csu/czso/r-spravni-obvody-obci-s-rozsirenou-pusobnosti-so-orp-moravskoslezskeho-kraje-kdvejpiwwn
    {'name': 'zlinsky.xlsx', 'url': 'https://www.czso.cz/documents/11284/17856167/33009921q1r06.xlsx/fec57ec5-a8de-4781-b418-ab6ec85b9728?version=1.1'} # https://www.czso.cz/csu/xz/vybrane_udaje_za_spravni_obvody_orp
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
