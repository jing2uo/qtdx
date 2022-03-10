import datetime
import os

from struct import unpack


class TdxDayFile:
    def _read(self, file: str):
        symbol = file.split("/")[-1].split(".")[0]
        with open(file, "rb") as f:
            while buf := f.read(32):
                a = unpack("IIIIIfII", buf)
                yield {
                    "date": datetime.datetime.strptime(
                        str(a[0]), "%Y%m%d"
                    ).isoformat(),  # 年月日
                    "open": a[1] / 100,  # 开盘价*100，int
                    "high": a[2] / 100,  # 最高价*100, int
                    "low": a[3] / 100,  # 最低价*100, int
                    "close": a[4] / 100,  # 收盘价*100, int
                    "amount": a[5],  # 成交额（元），float
                    "volume": a[6],  # 成交量（股），int
                    "symbol": symbol,
                }

    def convert_to_csv(self, dayfile: str):
        csv_head = """symbol,open,high,low,close,amount,volume,date \n"""
        csv_filepath = dayfile.replace(".day", ".csv")
        if os.path.exists(dayfile):
            with open(csv_filepath, "w") as f:
                f.write(csv_head)
                for i in self._read(dayfile):
                    t = (
                        i["symbol"],
                        i["open"],
                        i["high"],
                        i["low"],
                        i["close"],
                        i["amount"],
                        i["volume"],
                        i["date"],
                    )
                    f.write(",".join(str(x) for x in t) + "\n")

        return csv_filepath


dayfile = TdxDayFile()
