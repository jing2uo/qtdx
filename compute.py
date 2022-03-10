import talib
from db import stock, gbbq
from tdx import fq


def get_ma(symbol):
    bfq_data = stock.query(symbol=symbol)
    xdxr_data = gbbq.query(code=symbol[2:])
    df = fq(bfq_data, xdxr_data)
    df["Ma5"] = df.close.rolling(window=5).mean()
    df["Ma10"] = df.close.rolling(window=10).mean()
    df["Ma20"] = df.close.rolling(window=20).mean()
    df["Ma30"] = df.close.rolling(window=30).mean()
    df["Ma60"] = df.close.rolling(window=60).mean()
    return df


def get_macd(symbol):
    bfq_data = stock.query(symbol=symbol)
    xdxr_data = gbbq.query(code=symbol[2:])
    df = fq(bfq_data, xdxr_data)
    macd_dif, macd_dea, macd_bar = talib.MACD(
        df["close"].values, fastperiod=12, slowperiod=26, signalperiod=9
    )
    return macd_dif, macd_dea, macd_bar
