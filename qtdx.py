from datasource.eastmoney import *
from datasource.sina import *
from db.model import StockBase
import multiprocessing


def update_stock_status(exchange, code):
    s = get_stock_state_and_capital(str(exchange)+str(code))['status']
    StockBase.update({StockBase.status: s}).where(
        StockBase.code == code).execute()


def do():
    p = multiprocessing.Pool()
    for i in StockBase.select():
        p.apply_async(update_stock_status, (i.exchange, i.code))
    p.close()
    p.join()
