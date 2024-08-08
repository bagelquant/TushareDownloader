"""
Save and logging to database

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

from sqlalchemy import Engine, text
from pandas import DataFrame, to_datetime
from typing import Literal
from datetime import datetime


class SaverLogger:
    """
    save and log to database
    """

    def __init__(self,
                 engine: Engine,
                 table_name: str,
                 df: DataFrame) -> None:
        self.engine = engine
        self.table_name = table_name
        self.df = df

    def save(self,
             if_exists: Literal['fail', 'replace', 'append']) -> None:
        """save DataFrame to database"""
        # parse dates in DataFrame
        parse_names = ('date',
                       'cal_date',
                       'list_date',
                       'delist_date',
                       'trade_date',
                       'ann_date',
                       'f_ann_date',
                       'end_date',)
        for col in self.df.columns:
            if col in parse_names:
                self.df[col] = to_datetime(self.df[col])

        # save to database
        self.df.to_sql(self.table_name, con=self.engine, if_exists=if_exists, index=False)

    def log(self,
            message: Literal['success', 'fail'],
            ts_code: str,
            end_date: datetime) -> None:
        """log message to database"""
        with self.engine.begin() as con:
            con.execute(text(
                f"""
                INSERT INTO log (table_name, message, ts_code, end_date)
                VALUES ('{self.table_name}', '{message}', '{ts_code}', '{end_date}')
                """
            ))


def create_log_table(engine: Engine) -> None:
    """create log table if not exists"""
    with engine.begin() as con:
        con.execute(text(
            """
            CREATE TABLE IF NOT EXISTS log (
                id INT AUTO_INCREMENT PRIMARY KEY,
                table_name VARCHAR(50) NOT NULL,
                message ENUM('success', 'fail') NOT NULL,
                ts_code VARCHAR(20) NOT NULL,
                end_date DATETIME NOT NULL,
                update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        ))

