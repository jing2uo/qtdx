import os
import warnings
from datetime import datetime
from typing import List

import pandas as pd
import psycopg2
import requests
from psycopg2.extras import execute_values

from core.config import quest

warnings.simplefilter("ignore", UserWarning)


class QuestBase:
    def __init__(self) -> None:
        self.dsn = quest.dsn
        self.host = quest.host
        self.rest_port = quest.rest_port

    def sql_exec(self, sql: str):
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                try:
                    return cursor.fetchall()
                except:
                    pass


class StockDay(QuestBase):
    def bulk_insert_csv(self, file_path: str) -> None:
        if os.path.exists(file_path):
            schema = """[{"name": "symbol",  "type": "symbol"},
                         {"name": "open",    "type": "double"},
                         {"name": "high",    "type": "double"},
                         {"name": "low",     "type": "double"},
                         {"name": "close",   "type": "double"},
                         {"name": "amount",  "type": "double"},
                         {"name": "volume",  "type": "double"},
                         {"name": "date",    "type": "TIMESTAMP", "pattern": "yyyy-MM-ddTHH:mm:ss"}]"""
            params = {'name': 'trade'}
            files = {
                'schema': schema,
                'data': open(file_path, 'rb'),
            }
            requests.post('http://{host}:{port}/imp'.format(
                host=self.host, port=self.rest_port),
                params=params, files=files)

    def current_date(self):
        sql = '''SELECT date FROM 'trade'
                 WHERE symbol = 'sh999999'
                 ORDER BY date DESC
                 LIMIT 1;'''
        date = self.sql_exec(sql)[0][0]
        return date

    def format_sql(self, item: dict) -> tuple:
        return (
            item['symbol'],
            item['open'],
            item['high'],
            item['low'],
            item['close'],
            item['amount'],
            item['volume'],
            item['date']
        )

    def query(self, symbol, start_date):
        sql = '''
            select symbol, open, high, low, close, volume, date
            from trade where symbol = '{symbol}' and
            date >= to_timestamp('{start_date}','yyyyMMdd');
            '''.format(symbol=symbol, start_date=start_date)

        with psycopg2.connect(self.dsn) as conn:
            data = pd.read_sql(sql, conn)
        if not data.empty:
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date')
            return data

    def create_table(self) -> None:
        sql = '''CREATE TABLE IF NOT EXISTS 'trade' (
                    symbol SYMBOL INDEX,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    amount DOUBLE,
                    volume DOUBLE,
                    date TIMESTAMP);
                '''
        self.sql_exec(sql)


class AdjFactor(QuestBase):
    def bulk_insert(self, items: List) -> None:
        sql = '''INSERT INTO 'adj_factor' (symbol, factor, date, factor_type)
                 VALUES %s;'''
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, items)

    def create_table(self):
        sql = '''CREATE TABLE IF NOT EXISTS 'adj_factor' (
                factor_type SYMBOL,
                symbol SYMBOL INDEX,
                factor STRING,
                date TIMESTAMP);'''
        self.sql_exec(sql)

    def recreate_table(self):
        sql = '''
        DROP TABLE 'adj_factor';
        '''
        self.sql_exec(sql)
        self.create_table()

    def format_sql(self, symbol, dict):
        def gen_date(date_str):
            return datetime.strptime(date_str, '%Y-%m-%d').isoformat()
        l = []

        for qfq in dict['qfq']:
            l.append(tuple([symbol, qfq['f'], gen_date(qfq['d']), 'qfq']))
        for hfq in dict['hfq']:
            l.append(tuple([symbol, hfq['f'], gen_date(hfq['d']), 'hfq']))
        return l

    def query(self, symbol, factor_type):
        sql = '''
              SELECT * FROM 'adj_factor'
              WHERE symbol = '{symbol}'
              AND factor_type = '{factor_type}';
              '''.format(symbol=symbol, factor_type=factor_type)

        with psycopg2.connect(self.dsn) as conn:
            data = pd.read_sql(sql, conn)
        if not data.empty:
            data['date'] = pd.to_datetime(data['date'])
            data = data.set_index('date')
            return data


sd = StockDay()
af = AdjFactor()
