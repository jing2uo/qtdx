
from peewee import *

database = SqliteDatabase('stock.db')


class BaseModel(Model):
    class Meta:
        database = database


class SystemInfo(Model):
    init_finished = BooleanField(default=False)
    current_date = DateField()
    work_dir = CharField(default='/tmp/qtdx')


class StockBase(BaseModel):
    code = CharField(unique=True)
    name = CharField()
    listed_date = DateField(null=True)
    exchange = CharField()
    market = CharField()
    if_hksc = BooleanField()


StockBase.create_table()
