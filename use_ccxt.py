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
import argparse
import time

SETTINGS['database.name'] = 'postgresql'
SETTINGS['database.database'] = 'testdb'
SETTINGS['database.host'] = 'localhost'
SETTINGS['database.port'] = 5432
SETTINGS['database.user'] = 'zhaoru'

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
	Interval.MINUTE: timedelta(minutes=1),
    Interval.HOUR: timedelta(hours=1),
	Interval.DAILY: timedelta(days=1)         # 日线无需进行调整
}
CHINA_TZ = ZoneInfo("Asia/Shanghai")
LOADING_BAR_NUMBER_NUM = 10000

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
	print(df)
	if len(df) > 0:
		print(df.iloc[0])
		print("...{} lines", len(df) - 2)
		print(df.iloc[-1])
	time.sleep(bitfinex.rateLimit / 1000)
	return df

def save_df_to_database(db, df, symbol, exchange, interval):
	data: list[BarData] = []
	if df is not None:
		# 填充NaN为0
		df.fillna(0, inplace=True)
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
	db.save_bar_data(data)


def query_dataframe_and_save_to_database(db, start, end, interval, symbol, exchange):
	step_in_seconds = INTERVAL_ADJUSTMENT_MAP[interval].total_seconds()
	total_bars = (end - start) / step_in_seconds
	starttimestamp = start * 1000
	endtimestamp = end * 1000
	
	bar_loaded = 0
	while (starttimestamp < endtimestamp):
		left_bars = total_bars - bar_loaded
		current_bars = LOADING_BAR_NUMBER_NUM
		if (left_bars < LOADING_BAR_NUMBER_NUM):
			current_bars = left_bars
		interval_str = interval.value
		if interval == Interval.DAILY:
			interval_str = "1d"
		df = get_candle_bars_from_ccxt(symbol, interval_str, starttimestamp, current_bars)
		if df is None or len(df) == 0:
			break
		starttimestamp += (step_in_seconds * current_bars * 1000)
		enddftime = datetime.timestamp(df.iloc[-1]['date']) * 1000
		# The start time maybe too early, we need to adjust it
		if (enddftime > starttimestamp):
			starttimestamp = enddftime
		save_df_to_database(db, df, symbol, exchange, interval)
		bar_loaded += current_bars
	
	print("Total loaded {} bars: interval {}".format(bar_loaded, interval_str))
	
if __name__ == '__main__':
	timeformatstr = "%Y%m%d%H%M%S"
	parser = argparse.ArgumentParser(description='Get interval bar data of symbols and load them into database.')
	parser.add_argument('--symbol', help="The symbol of the bar data you want to load", default='BTC/USDT')
	parser.add_argument('--interval', help='The interval of the bar data.(1d,1h,1m)', default='1m')
	parser.add_argument('--starttime', help='Start datetime of your bar, format(YmdHMS)', default="20130101000000")
	parser.add_argument('--endtime', help='End datetime of your bar, format(YmdHMS)')
	args = parser.parse_args()
	interval_data = Interval.MINUTE
	if args.interval == '1D' or args.interval == '1d':
		interval_data = Interval.DAILY
	elif args.interval == '1h' or args.interval == '1H':
		interval_data = Interval.HOUR
	elif args.interval == '1M' or args.interval == '1m':
		pass
	else:
		print('The input interval {} is not a valid one, valid values could be (1d,wh,1m)')
		sys.exit()
	start_timestamp = datetime.strptime(args.starttime, timeformatstr)
	if args.endtime is None:
		end_timestamp = datetime.now()
	else:
		end_timestamp = datetime.strptime(args.endtime, timeformatstr)
	
	req = HistoryRequest(
		# 合约代码（示例cu888为米筐连续合约代码，仅用于示范，具体合约代码请根据需求查询数据服务提供商）
		# symbol="cu888",
		symbol = args.symbol,
		# 合约所在交易所
		exchange = Exchange.LOCAL,
		# exchange=Exchange.SHFE,
		# 历史数据开始时间
		start=start_timestamp,
		# 历史数据结束时间
		end=end_timestamp,
		# 数据时间粒度，默认可选分钟级、小时级和日级，具体选择需要结合该数据服务的权限和需求自行选择
		interval=interval_data
	)
	db = get_database()
	download_bar_data(db, req)

	
	# Get sample data from bitfinex using ccxt and save to BTC_USDT_1m.csv
	# start_time = datetime.timestamp(datetime(2021, 1, 1)) * 1000
	# end_time = datetime.timestamp(datetime(2021, 12, 31)) * 1000
	# df = get_candle_bars_from_ccxt("BTC/USDT", "1m", start_time, (end_time - start_time) / 60000)
	# df.to_csv("BTC_USDT_1m.csv")
	# print(df)

	# load df from csv
	# df = pd.read_csv("BTC_USDT_1m.csv")
	# print(type(df.loc[0, 'date']))
	# df['date'] = pd.to_datetime(df['date'])
	# print(type(df.loc[0, 'date']))
	# save_df_to_database(db, df.iloc[:10], "BTC/USDT", Exchange.LOCAL, Interval.MINUTE)



