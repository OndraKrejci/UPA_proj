
import requests
import os
import sys

DATA_PATH = 'data'

DATA = [
    {'name': 'osoby.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/osoby.json'},
    {'name': 'vyleceni.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/vyleceni.json'},
    {'name': 'umrti.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/umrti.json'},
    {'name': 'hospitalizace.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/hospitalizace.json'},
    {'name': 'nakazeni-vyleceni-umrti-testy.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/nakazeni-vyleceni-umrti-testy.json'},
    {'name': 'kraj-okres-nakazeni-vyleceni-umrti.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/kraj-okres-nakazeni-vyleceni-umrti.json'},
    {'name': 'incidence-7-14-kraje.json', 'url': 'https://onemocneni-aktualne.mzcr.cz/api/v2/covid-19/incidence-7-14-kraje.json'},
    {'name': 'zemreli-cr.csv', 'url': 'https://www.czso.cz/documents/62353418/155512389/130185-21data101921.csv/06c23c55-9c1a-4925-8386-8cd9625787ef?version=1.1'},
    {'name': 'obyvatelstvo-kraj-okres.csv', 'url': 'https://www.czso.cz/documents/62353418/143522504/130142-21data043021.csv/760fab9c-d079-4d3a-afed-59cbb639e37d?version=1.1'},
]

def ensure_folder(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def download(url: str, name: str, rewrite: bool = False, log: bool = True) -> None:
    path = '%s/%s' % (DATA_PATH, name)

    if not rewrite and os.path.isfile(path):
        if log:
            print('Skipping: %s' % name)
        return

    data = requests.get(url, allow_redirects=True)

    if data.status_code != 200:
        print(data.headers, file=sys.stderr)
        sys.exit()

    file = open(path, 'wb')
    file.write(data.content)
    file.close()

    if log:
        print('Downloaded: %s' % name)

def download_data(rewrite: bool = False, log: bool = True):
    ensure_folder(DATA_PATH + '/')

    for data in DATA:
        download(data['url'], data['name'], rewrite, log)

if __name__ == '__main__':
    download_data()
