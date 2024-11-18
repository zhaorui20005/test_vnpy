import ccxt
from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go

from datetime import datetime
from typing import List
from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData, TickData, HistoryRequest
from vnpy.trader.utility import round_to, ZoneInfo
from vnpy.trader.setting import SETTINGS
from importlib import import_module
from types import ModuleType

SETTINGS['database.name'] = 'postgresql'
SETTINGS['database.database'] = 'testdb'
SETTINGS['database.host'] = 'localhost'
SETTINGS['database.port'] = 5432
SETTINGS['database.user'] = 'zhaoru'

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
	Interval.MINUTE: timedelta(minutes=1),
    	Interval.HOUR: timedelta(hours=1),
	Interval.DAILY: timedelta()         # 日线无需进行调整
}
CHINA_TZ = ZoneInfo("Asia/Shanghai")
LOADING_BAR_NUMBER_NUM = 200000
def test_print_candleline():
	bitfinex = ccxt.bitfinex()
	starttimestamp = datetime.timestamp(datetime.strptime("20120101", "%Y%m%d")) * 1000
	candleline = bitfinex.fetchOHLCV('BTCUSD', '1d', starttimestamp, 6000)
	df = pd.DataFrame(candleline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volumn'])
	print("Start date is {}, end date is {}", datetime.fromtimestamp(df['timestamp'][0]/1000), datetime.fromtimestamp(df['timestamp'][len(df)-1]/1000))
	df['date']  = (df.timestamp/1000).apply(datetime.fromtimestamp)
	fig = go.Figure(data=[go.Candlestick(x=df['date'],open=df['open'], close=df['close'], high=df['high'], low=df['low'])])	
	fig.show()
	return df

def download_bar_data(db, req):
	symbol: str = req.symbol
	exchange: Exchange = req.exchange
	start: datetime = req.start
	end: datetime = req.end
	interval: Interval = req.interval
	if not interval:
        	interval = Interval.MINUTE
	end += timedelta(1)
	start = datetime.timestamp(start)
	end = datetime.timestamp(end)
	query_dataframe_and_save_to_database(db, start, end, interval, symbol, exchange)

# We just use bitfinex as an example here.
def get_candle_bars_from_ccxt(symbol, interval, start, count):
	bitfinex = ccxt.bitfinex()
	candleline = bitfinex.fetchOHLCV(symbol, interval, start, count)
	df = pd.DataFrame(candleline, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
	df['date']  = (df.timestamp/1000).apply(datetime.fromtimestamp)
	print("length of df is {}".format(len(df)))
	print(df)
	# print(df[0])
	# print(df[len(df)-1])
	return df

def save_df_to_database(db, df, symbol, exchange, interval):
	print(df)
	data: list[BarData] = []
	if df is not None:
		# 填充NaN为0
		#df.fillna(0, inplace=True)
		for row in df.itertuples():
			bar: BarData = BarData(
        		symbol=symbol,
        		exchange=exchange,
        		interval=interval,
        		datetime=row.date.replace(tzinfo=CHINA_TZ),
        		open_price=round_to(row.open, 0.000001),
        		high_price=round_to(row.high, 0.000001),
        		low_price=round_to(row.low, 0.000001),
        		close_price=round_to(row.close, 0.000001),
        		volume=row.volume,
        		turnover=0,
        		open_interest=0,
        		gateway_name="ccxt"
        	)
			data.append(bar)
			print(bar.__dict__)
	db.save_bar_data(data)


def query_dataframe_and_save_to_database(db, start, end, interval, symbol, exchange):
	total_bar = (end - start) / INTERVAL_ADJUSTMENT_MAP[interval].total_seconds()
	starttimestamp = start * 1000
	endtimestamp = end * 1000
	df = get_candle_bars_from_ccxt(symbol, interval.value, starttimestamp, total_bar)
	# save_df_to_database(db, df, symbol, exchange, interval)
	#endtimestamp = endtimestamp + step
	#each_bars = total_bar / LOADING_BAR_NUMBER_NUM
	#start = starttimestamp
	#for 
	
if __name__ == '__main__':
	# req = HistoryRequest(
	# 	# 合约代码（示例cu888为米筐连续合约代码，仅用于示范，具体合约代码请根据需求查询数据服务提供商）
	# 	# symbol="cu888",
	# 	symbol = "BTC/USDT",
	# 	# 合约所在交易所
	# 	exchange = Exchange.LOCAL,
	# 	# exchange=Exchange.SHFE,
	# 	# 历史数据开始时间
	# 	start=datetime(2024, 11, 1),
	# 	# 历史数据结束时间
	# 	end=datetime.now(),
	# 	# 数据时间粒度，默认可选分钟级、小时级和日级，具体选择需要结合该数据服务的权限和需求自行选择
	# 	interval=Interval.MINUTE
	# )
	db = get_database()
	# download_bar_data(db, req)
	# Get sample data from bitfinex using ccxt and save to BTC_USDT_1m.csv
	# start_time = datetime.timestamp(datetime(2021, 1, 1)) * 1000
	# end_time = datetime.timestamp(datetime(2021, 12, 31)) * 1000
	# df = get_candle_bars_from_ccxt("BTC/USDT", "1m", start_time, (end_time - start_time) / 60000)
	# df.to_csv("BTC_USDT_1m.csv")
	# print(df)

	# load df from csv
	df = pd.read_csv("BTC_USDT_1m.csv")
	print(type(df.loc[0, 'date']))
	df['date'] = pd.to_datetime(df['date'])
	print(type(df.loc[0, 'date']))
	save_df_to_database(db, df.iloc[:10], "BTC/USDT", Exchange.LOCAL, Interval.MINUTE)



