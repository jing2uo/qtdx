import datetime
import requests
import subprocess
import os

from subprocess import check_output, Popen, CalledProcessError
from struct import unpack
from typing import NewType

from utils import unzip_to_dir, download_file_to_dir, clean_dir, list_dir
from config import work_dir

from db import db

datestr = NewType('datestr', str())
"""
date: %Y%m%d
datestr: "20220218"
"""


class TdxDaily:
    def __init__(self) -> None:
        self.work_dir = work_dir
        self.url = None
        try:
            cmd = ['which', 'datatool']
            check_output(cmd)
        except CalledProcessError:
            raise FileNotFoundError('Please install datatool to PATH')

    def check_workday(self, day: datestr) -> bool:
        url = 'https://www.tdx.com.cn/products/data/data/g3day/{}.zip'.format(
            day)
        if requests.head(url).status_code == 200:
            self.url = url
            return True
        else:
            return False

    def get_day_file(self, day: datestr = None) -> tuple:
        if not day:
            day = datetime.date.today().strftime('%Y%m%d')

        if self.check_workday(day):
            day_dir = work_dir.rstrip('/') + '/' + day
            clean_dir(day_dir)
            sh_day_dir = day_dir + '/sh/lday'
            sz_day_dir = day_dir + '/sz/lday'

            dt_ini = """[PATH]\nVIPDOC={}\n""".format(day_dir)
            with open(day_dir + '/datatool.ini', 'w') as f:
                f.write(dt_ini)

            filepath = download_file_to_dir(self.url, day_dir, check_url=False)
            zip_extract_to = day_dir + '/refmhq'
            unzip_to_dir(filepath, zip_extract_to)
            p = Popen(
                ['datatool', 'day', 'create', day],
                cwd=day_dir,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.STDOUT)
            p.wait()
            return sh_day_dir, sz_day_dir

    def read_day_file(self, file: str):
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

    def day_to_csv(self, file: str):
        csv_head = '''symbol,open,high,low,close,amount,volume,date \n'''
        csv_filepath = file.replace('.day', '.csv')
        if os.path.exists(file):
            with open(csv_filepath, 'w') as f:
                f.write(csv_head)
                for i in self.read_day_file(file):
                    t = db.format_sql(i)
                    f.write(",".join(str(x) for x in t) + '\n')

    def dir_to_db(self, dir: str):
        if os.path.exists(dir):
            for f in list_dir(dir):
                self.day_to_csv(f)
                c = f.replace('.day', '.csv')
                db.bulk_insert_csv(c)

    def oneday(self, day: str = None):
        for dir in self.get_day_file(day):
            self.dir_to_db(dir)


t = TdxDaily()
