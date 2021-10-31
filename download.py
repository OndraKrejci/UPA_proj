
import requests
import os

DATA_PATH = 'data'

URL = {
    'name': 'url'
}

def ensure_folder(path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

def download(url: str, name: str) -> bool:
    data = requests.get(url, allow_redirects=True)

    if data.status_code != 200:
        return False

    file = open("%s/%s" % (DATA_PATH, name), 'wb')
    file.write(data.content)
    file.close()

    return True

if __name__ == '__main__':
    ensure_folder(DATA_PATH + '/')

    #download(URL['name'], 'name')
