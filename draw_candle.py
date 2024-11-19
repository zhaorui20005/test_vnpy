import plotly.graph_objects as go
import argparse

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import get_database
from vnpy.trader.object import BarData
from datetime import datetime, timedelta
from typing import List
import pandas as pd
import sys
from vnpy.trader.setting import SETTINGS


SETTINGS['database.name'] = 'postgresql'
SETTINGS['database.database'] = 'testdb'
SETTINGS['database.host'] = 'localhost'
SETTINGS['database.port'] = 5432
SETTINGS['database.user'] = 'zhaoru'

def print_candleline(Bars: List[BarData]):
    if len(Bars) == 0:
        print('No data to plot')
        return
    df = pd.DataFrame()
    for bar in Bars:
        new_df = pd.DataFrame({'date': bar.datetime, 
        'open': bar.open_price, 
        'high': bar.high_price, 
        'low': bar.low_price, 
        'close': bar.close_price, 
        'volumn': bar.volume}, index=[0])
        df = pd.concat([df, new_df])
    # https://plotly.com/python/candlestick-charts/
    fig = go.Figure(data=[go.Candlestick(x=df['date'],open=df['open'], close=df['close'], high=df['high'], low=df['low'])])	
    fig.show()
    return df

if __name__ == '__main__':
    timeformatstr = "%Y%m%d%H%M%S"
    parser = argparse.ArgumentParser(description='Get interval bar data of symbols and load them into database.')
    parser.add_argument('--symbol', help="The symbol of the bar data you want to load", default='BTC/USDT')
    parser.add_argument('--interval', help='The interval of the bar data.(1d,1h,1m)', default='1d')
    parser.add_argument('--starttime', help='Start datetime of your bar, format(YmdHMS)', default="20130101000000")
    parser.add_argument('--endtime', help='End datetime of your bar, format(YmdHMS)')
    args = parser.parse_args()
    interval_data = Interval.DAILY
    if args.interval == '1D' or args.interval == '1d':
        interval_data = Interval.DAILY
    elif args.interval == '1h' or args.interval == '1H':
        interval_data = Interval.HOUR
    elif args.interval == '1M' or args.interval == '1m':
        interval_data = Interval.MINUTE
    else:
        print('The input interval {} is not a valid one, valid values could be (1d,wh,1m)')
        sys.exit()
    start_timestamp = datetime.strptime(args.starttime, timeformatstr)
    if args.endtime is None:
        end_timestamp = datetime.now()
    else:
        end_timestamp = datetime.strptime(args.endtime, timeformatstr)
    db = get_database()
    bars = db.load_bar_data(args.symbol, Exchange.LOCAL, interval_data, start_timestamp, end_timestamp)
    print_candleline(bars)