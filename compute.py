import talib
from db.quest import StockDay

db = StockDay()


def get_ma(symbol, start_date):
    df = db.query(symbol=symbol, start_date=start_date)
    df['Ma5'] = df.close.rolling(window=5).mean()
    df['Ma10'] = df.close.rolling(window=10).mean()
    df['Ma20'] = df.close.rolling(window=20).mean()
    df['Ma30'] = df.close.rolling(window=30).mean()
    df['Ma60'] = df.close.rolling(window=60).mean()
    return df


def get_macd(df):
    macd_dif, macd_dea, macd_bar = talib.MACD(
        df['close'].values, fastperiod=12, slowperiod=26, signalperiod=9)
    return macd_dif, macd_dea, macd_bar


def get_kdj(df):
    kk, dd = talib.STOCH(df.high.values,
                         df.low.values,
                         df.close.values,
                         fastk_period=9,
                         slowk_period=3,
                         slowk_matype=0,
                         slowd_period=3,
                         slowd_matype=0)
    jj = 3 * kk - 2 * dd
