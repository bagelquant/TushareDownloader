"""
MySQL connection

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

from dataclasses import dataclass, field
from sqlalchemy import create_engine, Engine, text
from abc import ABC, abstractmethod


@dataclass(slots=True)
class Database(ABC):

    engine: Engine = field(init=False)

    def __post_init__(self):
        self._create_engine()

    @abstractmethod
    def _create_engine(self) -> None:
        ...

    @abstractmethod
    def create_index(self, table_name: str) -> None:
        """
        Create index for {table_name} if not exists:
        - ts_code
        - trade_date
        - f_ann_date
        """
        ...

    @abstractmethod
    def _check_index_exists(self, table_name: str) -> bool:
        ...


@dataclass(slots=True)
class MySQL(Database):

    host: str
    port: int
    user: str
    password: str
    database: str

    def _create_engine(self) -> None:
        self.engine = create_engine(
            f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        )

    def _check_index_exists(self, table_name: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(text(f"SHOW INDEX FROM {table_name}"))
            return result.rowcount > 0

    def create_index(self, table_name: str) -> None:
        # check if index exists
        if self._check_index_exists(table_name):
            return
        with self.engine.begin() as conn:
            # query columns names for the table
            result = conn.execute(text(f"SHOW COLUMNS FROM {table_name}"))
            columns = [row[0] for row in result]

            if "ts_code" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_ts_code ON {table_name}(ts_code(10))"))
            if "trade_date" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_trade_date ON {table_name}(trade_date)"))
            if "f_ann_date" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_f_ann_date ON {table_name}(f_ann_date)"))


@dataclass(slots=True)
class SQLite(Database):

    path: str

    def _create_engine(self) -> None:
        self.engine = create_engine(f"sqlite:///{self.path}")

    def _check_index_exists(self, table_name: str) -> bool:
        with self.engine.begin() as conn:
            result = conn.execute(text(f"PRAGMA index_list({table_name})"))
            return result.rowcount > 0

    def create_index(self, table_name: str) -> None:
        # check if index exists
        if self._check_index_exists(table_name):
            return
        with self.engine.begin() as conn:
            # query columns names for the table
            result = conn.execute(text(f"PRAGMA table_info({table_name})"))
            columns = [row[1] for row in result]

            if "ts_code" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_ts_code ON {table_name}(ts_code)"))
            if "trade_date" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_trade_date ON {table_name}(trade_date)"))
            if "f_ann_date" in columns:
                conn.execute(text(f"CREATE INDEX idx_{table_name}_f_ann_date ON {table_name}(f_ann_date)"))

