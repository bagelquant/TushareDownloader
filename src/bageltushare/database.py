"""
MySQL connection

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

from sqlalchemy import create_engine, Engine, text
from abc import ABC, abstractmethod


class Database(ABC):
    """database base"""

    @abstractmethod
    def get_engine(self) -> Engine:
        """get a sqlalchemy engine"""
        ...

    @abstractmethod
    def create_index(self, table_name: str) -> None:
        """create index for all table, index columns: ts_code, trade_date, f_ann_date"""
        ...


class MySQL(Database):
    """
    MySQL connection
    """

    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.engine = self._create_engine()

    def _create_engine(self):
        return create_engine(
            f'mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )

    def get_engine(self) -> Engine:
        return self.engine

    def create_index(self, table_name: str) -> None:
        with self.engine.begin() as con:
            # query columns names for the table
            result = con.execute(text(f'SHOW COLUMNS FROM {table_name}'))
            columns = [row[0] for row in result]

            if 'ts_code' in columns:
                con.execute(text(f'CREATE INDEX idx_{table_name}_ts_code ON {table_name}(ts_code(10))'))
            if 'trade_date' in columns:
                con.execute(text(f'CREATE INDEX idx_{table_name}_trade_date ON {table_name}(trade_date)'))
            if 'f_ann_date' in columns:
                con.execute(text(f'CREATE INDEX idx_{table_name}_f_ann_date ON {table_name}(f_ann_date)'))
