import psycopg2
import requests
import os
from psycopg2.extras import execute_values
from typing import List
from config import quest


class DB:
    def __init__(self) -> None:
        self.dsn = quest.dsn
        self.table = quest.table
        self.host = quest.host
        self.rest_port = quest.rest_port

    def sql_exec(self, sql: str):
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql)

    # slow
    def bulk_insert(self, items: List) -> None:
        sql = '''INSERT INTO {table} VALUES %s;'''.format(table=self.table)
        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, sql, items)

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
            params = {'name': self.table}
            files = {
                'schema': schema,
                'data': open(file_path, 'rb'),
            }
            requests.post('http://{host}:{port}/imp'.format(
                host=self.host, port=self.rest_port),
                params=params, files=files)

    def query(self,):
        pass

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

    def db_init(self) -> None:
        sql = '''CREATE TABLE {table} (
                    symbol SYMBOL INDEX,
                    open DOUBLE,
                    high DOUBLE,
                    low DOUBLE,
                    close DOUBLE,
                    amount DOUBLE,
                    volume DOUBLE,
                    date TIMESTAMP
                );
                PARTITION BY DAY;
                '''.format(table=self.table)
        self.sql_exec(sql)


db = DB()
