"""
query stock_list, trade_cal and lastest_date in database

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

from sqlalchemy import Engine, text
from datetime import datetime


class Query:
    """
    query stock_list, trade_cal and lastest_date in database
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def stock_list(self) -> list[str]:
        """query stock list from database"""
        with self.engine.connect() as con:
            result = con.execute(text('SELECT ts_code FROM stock_basic')).fetchall()
            return [row[0] for row in result]

    def trade_cal(self, start_date: datetime, end_date: datetime) -> list[datetime]:
        """query trade calendar from database sort by cal_date"""
        with self.engine.connect() as con:
            result = con.execute(text(f"""
            SELECT cal_date 
            FROM trade_cal 
            WHERE is_open=1 
            AND cal_date BETWEEN '{start_date.strftime('%Y%m%d')}' AND '{end_date.strftime('%Y%m%d')}'
            ORDER BY cal_date
            """)).fetchall()
            return [row[0] for row in result]

    def end_date_in_log(self, api_name: str, ts_code: str) -> datetime | None:
        """query end_date in log table"""
        with self.engine.connect() as con:
            result = con.execute(text(
                f"""
                SELECT MAX(end_date)
                FROM log
                WHERE table_name='{api_name}' AND ts_code='{ts_code}' 
                AND message='success'
                """
            )).fetchone()
            return result[0]