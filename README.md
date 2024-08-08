# TushareDownloader
A packege for download China A market stock data using tushare api, and automatically store to a local database 

Full documents please refer to: [BagelQuant](https://bagelquant.com/tusharedownloader)

# Installation

```bash
pip install TushareDownloader
```

# Usage

## Quick start

```python
from TushareDownloader import MySQL, create_log_table, update_and_replace

# Initialize a MySQL object, will support other database in upcoming release
mysql_config = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '<PASSWORD>',
    'database': 'tushare',
}
mysql = MySQL(**mysql_config)

# create log table, only needed at first time use
create_log_table(engine=mysql.get_engine())

# download stock_basic
token: str = '<YOUR TUSHARE TOKEN>'
update_and_replace(token=token,             # required
                   database=mysql,       # required
                   api_name='stock_basic',  # required
                   list_status='L, D',      # optional
                   fields=[                 # optional
                           "ts_code",
                           "symbol",
                           "name",
                           "area",
                       ])
```

The `update_and_replace` function utilizes the tushare API to download data, which is then automatically stored in a local database. There's no need to create a database table structure, as the table name will be the same as the api_name.

> Be aware that this function will replace the original data in the database
>

**This function has three required parameters:**

- `token: str` , the tushare token used to access data
- `database: Database` , a `TushareDownloader.Database` object, in this case, a `MySQL` object
    - this is where the data is stored
- `api_name: str` , the tushare API name. For more information, please refer to the tushare documents [Tushare Doc](https://tushare.pro/document/2)

## Loop update

---

This package provides two methods for multi-threaded looping downloads:

### `date_loop_update`

```python
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
    """
    ...
```

> Initially, this method requires the **trade_cal** table. Use `update_and_replace` to download the **trade_cal** table.
>

The function process includes:

1. Querying the latest `trade_date` in the local database for the corresponding `api_name`.
2. Downloading data for each `trade_date` until today, using a multi-threading method.
3. Appending new data to the local database.

**Best use case:**

- Use `date_loop_update` data to automatically update price data **(update to today)**

```python
def daily_update():
    """daily update price data"""
    
    # all api names to download
    date_loop_api_names = (
    'daily',
    'adj_factor',
		)
		
		# download
    for api_name in date_loop_api_names:
        date_loop_update(token=TUSHARE_TOKEN, 
                         database=MYSQL, 
                         api_name=api_name)
```

### `code_loop_api_names`

```python
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
    """
    ...
```

> Initially, this method requires the **stock_basic** table. Use `update_and_replace` to download the **stock_basic** table.
>

The function process includes:

1. Querying the latest `stock_list` in the local database for the corresponding `api_name`.
2. Querying the latest `trade_date` in the local database for the corresponding `ts_code`
3. Downloading data for each `ts_code` using a multi-threading method, latest `trade_date` as start_date, today as end_date
4. Appending new data to the local database.

**Best use case:**

- Use `code_loop_update` data to automatically update fundamental data **(update to today)**

```python
def monthly_update():
    """daily update price data"""
    
    # all api names to download
    code_loop_api_names = (
    'balancesheet',
    'income',
    'cashflow',
		)
		
		# download
    for api_name in code_loop_api_names:
        code_loop_update(token=TUSHARE_TOKEN, 
                         database=MYSQL, 
                         api_name=api_name)
```

### Usage of Loop Update

- The loop update method will automatically update data to the present day.
- Both methods use multi-threading; this may exceed Tushare API usage limits per minute. If limits are exceeded, the looping will pause for one minute and then continue.
- Some Tushare APIs require a 'ts_code' as input. In such cases, use the 'code_loop_update' method. Example: balancesheet.
- For price data, which doesn't require 'ts_code' as input, use the 'date_loop_update' method.