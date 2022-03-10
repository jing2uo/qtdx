import requests
import zipfile
import shutil
import os
from requests.exceptions import HTTPError


def remove_dir(dir: str) -> None:
    if os.path.exists(dir):
        shutil.rmtree(dir, ignore_errors=True)


def clean_dir(dir: str) -> None:
    remove_dir(dir)
    os.makedirs(dir)


def list_dir(dir: str):
    yield from (
        os.path.abspath(os.path.join(dirpath, f))
        for dirpath, _, filenames in os.walk(dir)
        for f in filenames
    )


def download_file(url: str, target_path: str, check_url: bool = True):
    os.makedirs(target_path, exist_ok=True)
    if check_url:
        status_code = requests.head(url).status_code
    else:
        status_code = 200
    if status_code == 200:
        filename = url.split("/")[-1]
        filepath = os.path.join(target_path, filename)
        with requests.get(url, stream=True) as r:
            with open(filepath, "wb") as f:
                shutil.copyfileobj(r.raw, f)
        return filepath
    else:
        raise HTTPError(f"404: {url}")


def unzip_file(file_path: str, target_path: str, remove_file: bool = True) -> bool:
    if not os.path.exists(file_path):
        raise FileNotFoundError(file_path)

    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(target_path)

    if remove_file:
        os.remove(file_path)

    return True
