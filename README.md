# qtdx = Tdx + QuestDB



## 目的

- 获取 A 股所有股票的历史日线
- 提供快速且不受限制的查询接口

- 数据存在本地，每天定时更新

## 数据来源

- [通达信](https://www.tdx.com.cn/article/datacenter.html) 
  - 提供了沪深所有证券的历史日线下载，数据完整、稳定。
  - 缺失股票的基本信息
  - 缺失复权数据

- [新浪财经](https://finance.sina.com.cn)
  - 行情中心的 js 可以获取到股票的复权因子、股票状态和市值

- [东方财富](https://quote.eastmoney.com/center/gridlist.html#hs_a_board)
  - 行情中心可以更简单的获取到股票列表、是否港股通的信息

