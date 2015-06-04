__author__ = 'Ruoqian Liu'
import argparse
import time
import os
from pandas import read_csv
from pandas.io.data import DataReader
from datetime import datetime


def get_price_data(sym_list_file_name, start_time, end_time, result_dir):
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    field = 'Adj Close'
    symbol_frame = read_csv(sym_list_file_name)
    num_of_symbols = symbol_frame.ticker.shape[0]
    for idx in xrange(num_of_symbols):
        sym = symbol_frame.ticker[idx]
        try:
            print '... get price %s' % sym
            price_frame = DataReader(sym, 'yahoo', start_time, end_time)
        except Exception as e:
            print(e.message)
            print '... Warning: No Price for %s' % sym
            continue
        result_fn = '%s/%s' % (result_dir, sym)
        price_frame[field].to_csv(result_fn)


def get_volume_data(sym_list_file_name, start_time, end_time, result_dir):
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    field = 'Volume'
    symbol_frame = read_csv(sym_list_file_name)
    num_of_symbols = symbol_frame.ticker.shape[0]
    for idx in xrange(num_of_symbols):
        sym = symbol_frame.ticker[idx]
        try:
            print '... get volume %s' % sym
            price_frame = DataReader(sym, 'yahoo', start_time, end_time)
        except Exception as e:
            print(e.message)
            print '... Warning: No Volume for %s' % sym
            continue
        result_fn = '%s/%s' % (result_dir, sym)
        price_frame[field].to_csv(result_fn)


def int2date(intformat):
    year = intformat / 10000
    month = intformat % 10000 / 100
    day = intformat % 10000 % 100
    return datetime(year, month, day)


if __name__ == '__main__':
    now = int(time.strftime("%Y%m%d", time.localtime(time.time())))
    result_path = './data'
    argparser = argparse.ArgumentParser()
    argparser.add_argument('option', type=str,
                            choices=['price', 'volume'],
                            help='what kind of data to fetch')
    argparser.add_argument('-s', '--start', type=int, default=20050101,
                           help='The start date (default: 20050101)')
    argparser.add_argument('-e', '--end', type=int, default=now,
                           help='The end date (default: {0})'.format(now))
    argparser.add_argument('-p', '--path', type=str, default=result_path,
                           help='Path to store the data (default: {0})'.format(result_path))
    args = argparser.parse_args()
    if args.option == 'price':
        get_price_data(sym_list_file_name='./data/SP100List.csv',
                start_time=int2date(args.start),
                end_time=int2date(args.end),
                result_dir=args.path)
    elif args.option == 'volume':
        get_volume_data(sym_list_file_name='./data/SP100List.csv',
                start_time=int2date(args.start),
                end_time=int2date(args.end),
                result_dir=args.path)
