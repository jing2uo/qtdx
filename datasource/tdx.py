import datetime
import requests
import subprocess
import os
import shutil
import pandas as pd
import multiprocessing

from subprocess import check_output, Popen, CalledProcessError
from struct import unpack
from typing import NewType

from utils import unzip_file_to_dir, download_file_to_dir, clean_dir, list_dir
from core.config import work_dir

from db.quest import StockDay

db = StockDay()

datestr = NewType('datestr', str())
"""
date: %Y%m%d
datestr: "20220218"
"""


class TdxDay:
    def __init__(self) -> None:
        self.work_dir = work_dir
        self.url = None
        try:
            cmd = ['which', 'datatool']
            check_output(cmd)
        except CalledProcessError:
            raise FileNotFoundError('Please install datatool to PATH')

    def check_if_workday(self, date: datestr) -> bool:
        url = 'https://www.tdx.com.cn/products/data/data/g3day/{}.zip'.format(
            date)
        if requests.head(url).status_code == 200:
            self.url = url
            return True
        else:
            return False

    def download_dayfiles(self, date: datestr = None) -> str:
        if not date:
            date = datetime.date.today().strftime('%Y%m%d')

        if self.check_if_workday(date):
            today_dir = work_dir.rstrip('/') + '/' + date
            clean_dir(today_dir)
            sh_day_dir = today_dir + '/sh/lday'
            sz_day_dir = today_dir + '/sz/lday'

            dt_ini = """[PATH]\nVIPDOC={}\n""".format(today_dir)
            with open(today_dir + '/datatool.ini', 'w') as f:
                f.write(dt_ini)

            filepath = download_file_to_dir(
                self.url, today_dir, check_url=False)
            zip_extract_to = today_dir + '/refmhq'
            unzip_file_to_dir(filepath, zip_extract_to)
            p = Popen(
                ['datatool', 'day', 'create', date],
                cwd=today_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            p.wait()

            aio = today_dir + '/allinone'
            p = Popen('mkdir -p {aio} && mv {sh_day_dir} {aio}/sh && mv {sz_day_dir} {aio}/sz'.format(
                aio=aio, sz_day_dir=sz_day_dir, sh_day_dir=sh_day_dir
            ), shell=True)
            p.wait()

            return aio

    def read_dayfile(self, file: str):
        symbol = file.split("/")[-1].split(".")[0]
        with open(file, 'rb') as f:
            while buf := f.read(32):
                a = unpack('IIIIIfII', buf)
                yield {'date': datetime.datetime.strptime(str(a[0]), "%Y%m%d").isoformat(),        # 年月日
                       'open': a[1]/100,    # 开盘价*100，int
                       'high': a[2]/100,    # 最高价*100, int
                       'low': a[3]/100,     # 最低价*100, int
                       'close': a[4]/100,   # 收盘价*100, int
                       'amount': a[5],      # 成交额（元），float
                       'volume': a[6],      # 成交量（股），int
                       'symbol': symbol}

    def convert_dayfile_to_csv(self, file: str):
        csv_head = '''symbol,open,high,low,close,amount,volume,date \n'''
        csv_filepath = file.replace('.day', '.csv')
        if os.path.exists(file):
            with open(csv_filepath, 'w') as f:
                f.write(csv_head)
                for i in self.read_dayfile(file):
                    t = db.format_sql(i)
                    f.write(",".join(str(x) for x in t) + '\n')

    def insert_dayfile(self, file: str):
        self.convert_dayfile_to_csv(file)
        c = file.replace('.day', '.csv')
        db.bulk_insert_csv(c)

    def insert_dayfiles_in_dir(self, dir: str):
        if os.path.exists(dir):
            p = multiprocessing.Pool()
            for f in list_dir(dir):
                p.apply_async(self.insert_dayfile, (f,))
            p.close()
            p.join()

    def oneday(self, date: datestr = None, clean=True):
        d = self.download_dayfiles(date)
        if d:
            self.insert_dayfiles_in_dir(d)
        if clean == True:
            shutil.rmtree(d, ignore_errors=True)

    def somedays(self, start: datestr, end: datestr = None):
        if end == None:
            end = datetime.date.today().strftime('%Y%m%d')
        date_list = pd.date_range(start, end).strftime("%Y%m%d").to_list()
        for date in date_list:
            self.oneday(date)

    def up_to_date(self):
        start_date = (db.current_date() +
                      datetime.timedelta(days=1)).strftime("%Y%m%d")
        self.somedays(start=start_date)

    def load_all_history(self, zipfile_path: str):
        if os.path.exists(zipfile_path):
            temp_dir = self.work_dir.rstrip('/') + '/' + 'temp'
            clean_dir(temp_dir)
            unzip_file_to_dir(file=zipfile_path,
                              dir=temp_dir, remove_file=False)
            t.insert_dayfiles_in_dir(dir=temp_dir)
            shutil.rmtree(temp_dir)


t = TdxDay()
