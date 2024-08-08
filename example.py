"""
Example for TushareDownloader
Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

from time import time
from TushareDownloader import MySQL, create_log_table, update_and_replace, date_loop_update, code_loop_update


def main():
    """example for TushareDownloader"""
    # create a mysql database object
    database = MySQL(
        host='localhost',
        port=3306,
        user='root',
        password='<password>',
        database='tushare'
    )

    # tushare token
    token = '<your token>'

    # create log table
    create_log_table(database.get_engine())

    # download stock_basic and replace table in database
    update_and_replace(token=token,  # required
                       database=database,  # required
                       api_name='stock_basic',  # required
                       list_status='L, D',  # optional
                       fields=[  # optional
                           "ts_code",
                           "symbol",
                           "name",
                           "area",
                           "industry",
                           "fullname",
                           "enname",
                           "cnspell",
                           "market",
                           "exchange",
                           "curr_type",
                           "list_status",
                           "list_date",
                           "delist_date",
                           "is_hs",
                           "act_name",
                           "act_ent_type"
                       ])
    update_and_replace(token=token,
                       database=database,
                       api_name='trade_cal')

    # loop date download, for price and volume data
    date_loop_api_names = (
        'daily',
        'adj_factor',
    )

    for api_name in date_loop_api_names:
        date_loop_update(token=token,
                         database=database,
                         api_name=api_name)

    # loop code download, for daily basic data
    code_loop_api_names = (
        'balancesheet',
        'cashflow',
        'income',
    )
    for api_name in code_loop_api_names:
        code_loop_update(token=token,
                         database=database,
                         api_name=api_name)

    # create index
    database.create_index('stock_basic')
    database.create_index('trade_cal')
    database.create_index('daily')
    database.create_index('adj_factor')
    database.create_index('balancesheet')
    database.create_index('cashflow')
    database.create_index('income')


if __name__ == '__main__':
    start = time()
    # download
    main()

    # create index

    time_elapsed = time() - start
    print(f'time elapsed: {time_elapsed:.2f} seconds, \n '
          f'{time_elapsed/60:.2f} minutes. \n '
          f'{time_elapsed/3600:.2f} hours.')
