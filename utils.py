import requests
import zipfile
import shutil
import os
from requests.exceptions import HTTPError


def clean_dir(dir: str) -> None:
    if os.path.exists(dir):
        shutil.rmtree(dir, ignore_errors=True)
    os.makedirs(dir)


def list_dir(dir: str):
    for dirpath, _, filenames in os.walk(dir):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def download_file_to_dir(url: str, dir: str, check_url: bool = True):
    if not os.path.exists(dir):
        os.makedirs(dir)
    if check_url:
        status_code = requests.head(url).status_code
    else:
        status_code = 200
    if status_code == 200:
        filename = url.split('/')[-1]
        filepath = dir.rstrip('/') + '/' + filename
        with requests.get(url, stream=True) as r:
            with open(filepath, 'wb') as f:
                shutil.copyfileobj(r.raw, f)
            return filepath
    else:
        raise HTTPError('404: ' + url)


def unzip_to_dir(file: str, dir: str, remove_file=True):
    if os.path.exists(file):
        with zipfile.ZipFile(file, "r") as zip_ref:
            zip_ref.extractall(dir)
        if remove_file is True:
            os.remove(file)
        return list_dir(dir)
    else:
        raise FileNotFoundError(file)
