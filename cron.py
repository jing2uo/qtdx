import datetime
import multiprocessing
import pandas as pd

from config import work_dir
from utils import get_logger, list_dir, remove_dir

from tdx import datatool, dayfile, datestr
from tdx import gbbq as tdx_gbbq
from db import stock
from db import gbbq as gbbq_db


logger = get_logger(__name__)


def update_gbbq():
    logger.info("开始更新 gbbq 表")
    gbbq_db.recreate_table()
    logger.info("重建 gbbq 表完成")
    logger.info("获取最新的 gbbq 文件")
    gbbq = tdx_gbbq.get_gbbq()
    logger.info("获取完成")
    gbbq_file = work_dir.rstrip("/") + "/gbbq.csv"
    gbbq.to_csv(gbbq_file, index=False)
    logger.info("导入 gbbq 文件到数据库")
    gbbq_db.bulk_insert_csv(file_path=gbbq_file)
    logger.info("导入完成")


def insert_dayfile(file: str):
    dayfile.convert_to_csv(file)
    csv = file.replace(".day", ".csv")
    stock.bulk_insert_csv(csv)


def oneday(date: datestr = None, clean=True):
    d = datatool.download_dayfile(date)
    if d != "not workday":
        p = multiprocessing.Pool()
        for f in list_dir(d):
            p.apply_async(insert_dayfile, (f,))
        p.close()
        p.join()
        if clean == True:
            remove_dir(d)
        return True
    else:
        return None


def update_stock():
    logger.info("开始更新 stock 表")
    sh_latest = stock.latest_data("sh999999").index[0]
    sz_latest = stock.latest_data("sz399001").index[0]
    bj_latest = stock.latest_data("bj899050").index[0]

    if not (sh_latest == sz_latest == bj_latest):
        logger.error(
            """数据库中证券日线数据不完整,
            请删除 questdb 所有数据后重新执行 prepare 初始化"""
        )
    else:
        latest_date = sh_latest.strftime("%Y%m%d")
        logger.info("数据库中证券日线数据最新日期是 {}".format(latest_date))

        start = (sh_latest + datetime.timedelta(days=1)).strftime("%Y%m%d")
        end = datetime.date.today().strftime("%Y%m%d")
        dates = pd.date_range(start, end)
        logger.info("现在是 {}".format(end))
        logger.info("共需要更新 {} 天数据".format(len(dates)))
        if not dates.empty:
            with multiprocessing.Pool() as p:
                r = p.map(oneday, dates.strftime("%Y%m%d").tolist())
            for i, date in enumerate(dates):
                if r[i]:
                    logger.info("{} 的数据更新完成".format(date.strftime("%Y-%m-%d")))
                if not r[i]:
                    logger.info("{} 非交易日".format(date.strftime("%Y-%m-%d")))
        else:
            logger.info("stock 表数据已经是最新，不需要更新")


if __name__ == "__main__":
    update_gbbq()
    update_stock()
