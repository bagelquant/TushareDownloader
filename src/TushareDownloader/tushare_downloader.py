"""
tushare_downloader main

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

import time
from .tushare_api import TushareAPI
from .save_and_log import SaverLogger
from .database import Database
from .query import Query

from datetime import datetime, timedelta
from pandas import DataFrame
from concurrent.futures import ThreadPoolExecutor


def update_and_replace(token: str,
                       database: Database,
                       api_name: str,
                       **kwargs) -> None:
    """
    Update and replace table in database
    :param token: Tushare token
    :param database: Database object with sqlalchemy engine
    :param api_name: tushare api name, same with table name in database
    :param kwargs: arguments for tushare api
    """
    try:
        # download data
        tushare_api = TushareAPI(token)
        df = tushare_api.download(api_name, **kwargs)

        # save and log
        saver_logger = SaverLogger(database.get_engine(), api_name, df)
        saver_logger.save('replace')
        saver_logger.log(message='success', ts_code='all', end_date=datetime.now())
        print(f'{api_name} updated and replaced')
    except Exception as e:
        saver_logger = SaverLogger(database.get_engine(), api_name, DataFrame())
        saver_logger.log(message='fail', ts_code='all', end_date=datetime.now())
        print(f'{api_name} update Error: {e}')


def _single_date_update(token: str,
                        database: Database,
                        api_name: str,
                        trade_date: datetime) -> None:
    """
    Single date update
    :param token: Tushare token
    :param database: Database object with sqlalchemy engine
    :param api_name: tushare api name, same with table name in database
    :param trade_date: trade date to update
    """
    try:
        # download data
        tushare_api = TushareAPI(token)
        df = tushare_api.download(api_name, trade_date=trade_date.strftime('%Y%m%d'))

        # save and log
        saver_logger = SaverLogger(database.get_engine(), api_name, df)
        saver_logger.save('append')
        saver_logger.log(message='success', ts_code='all', end_date=trade_date)
        print(f'{api_name} updated for {trade_date}')
    except Exception as e:
        saver_logger = SaverLogger(database.get_engine(), api_name, DataFrame())
        saver_logger.log(message='fail', ts_code='all', end_date=trade_date)
        print(f'{api_name}_{trade_date} update Error: {e} \n retrying in 60s...')

        # retry in 60s
        time.sleep(60)
        _single_date_update(token, database, api_name, trade_date)


def _single_code_update(token: str,
                        database: Database,
                        api_name: str,
                        ts_code: str) -> None:
    """
    Single code update
    :param token: Tushare token
    :param database: Database object with sqlalchemy engine
    :param api_name: tushare api name, same with table name in database
    :param ts_code: ts_code to update
    """
    try:
        # query last_date in database
        query = Query(database.get_engine())
        start_date = query.end_date_in_log(api_name, ts_code=ts_code)
        if start_date is None:
            start_date = datetime(2000, 1, 1)
        else:
            start_date += timedelta(days=1)
        end_date = datetime.now()

        # end_date need to later than start_date
        if end_date < start_date:
            print(f'{api_name} already up to date')
            return

        # download data
        tushare_api = TushareAPI(token)
        df = tushare_api.download(api_name,
                                  ts_code=ts_code,
                                  start_date=start_date.strftime('%Y%m%d'),
                                  end_date=end_date.strftime('%Y%m%d'))

        # save and log
        saver_logger = SaverLogger(database.get_engine(), api_name, df)
        saver_logger.save('append')
        saver_logger.log(message='success', ts_code=ts_code, end_date=end_date)
        print(f'{api_name} updated for {ts_code}')
    except Exception as e:
        saver_logger = SaverLogger(database.get_engine(), api_name, DataFrame())
        saver_logger.log(message='fail', ts_code=ts_code, end_date=datetime.now())
        print(f'{api_name}_{ts_code} update Error: {e} \n retrying in 60s...')

        # retry in 60s
        time.sleep(60)
        _single_code_update(token, database, api_name, ts_code)


def date_loop_update(token: str,
                     database: Database,
                     api_name: str,
                     max_workers: int = 10) -> None:
    """
    Loop date update
    :param token: Tushare token
    :param database: Database object with sqlalchemy engine
    :param api_name: tushare api name, same with table name in database
    :param max_workers: max workers for ThreadPoolExecutor

    query trade_cal
    loop through last date to today
    """
    # query last_date in database
    query = Query(database.get_engine())
    start_date = query.end_date_in_log(api_name, ts_code='all')
    if start_date is None:
        start_date = datetime(2000, 1, 1)
    else:
        start_date += timedelta(days=1)
    end_date = datetime.now()

    # end_date need to later than start_date
    if end_date < start_date:
        print(f'{api_name} already up to date')
        return
    # query trade_cal
    trade_cal = query.trade_cal(start_date, end_date)

    # loop through trade_cal using multi-thread
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for trade_date in trade_cal:
            executor.submit(_single_date_update, token, database, api_name, trade_date)


def code_loop_update(token: str,
                     database: Database,
                     api_name: str,
                     max_workers: int = 10) -> None:
    """
    Loop code update
    :param token: Tushare token
    :param database: Database object with sqlalchemy engine
    :param api_name: tushare api name, same with table name in database
    :param max_workers: max workers for ThreadPoolExecutor

    query stock_list
    """
    # query stock_list
    query = Query(database.get_engine())
    stock_list = query.stock_list()

    # loop through stock_list using multi-thread
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for ts_code in stock_list:
            executor.submit(_single_code_update, token, database, api_name, ts_code)
