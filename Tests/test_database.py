"""
Tests for mysql database

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""
import json
import os
import unittest
from unittest import TestCase

from src.bageltushare.database import MySQL, SQLite
from sqlalchemy import text


class TestMySQLDB(TestCase):

    def setUp(self):
        """initialize mysql db"""
        with open(os.path.join(os.path.dirname(__file__), "test_config.json")) as f:
            config = json.load(f)["database_config"]

        self.db = MySQL(**config)
        

    def _create_test_table(self):
        # create a test table
        with self.db.engine.begin() as conn:
            sql = text("CREATE TABLE IF NOT EXISTS test_table ("
                       "id INT AUTO_INCREMENT PRIMARY KEY,"
                       "trade_date DATE NOT NULL,"
                       "name VARCHAR(255) NOT NULL,"
                       "age INT NOT NULL"
                       ")")
            conn.execute(sql)

    def _drop_test_table(self):
        with self.db.engine.begin() as conn:
            sql = text("DROP TABLE IF EXISTS test_table")
            conn.execute(sql)

    def test_create_index(self):
        self._create_test_table()
        self.db.create_index("test_table")
        self._drop_test_table()
        print("MySQL index creation test passed")


class TestSQLite(TestCase):

    def setUp(self):
        """initialize sqlite db"""
        path: str = os.path.join(os.path.dirname(__file__), "test.db")
        self.db = SQLite(path=path)

    def _create_test_table(self):
        # create a test table
        with self.db.engine.begin() as conn:
            sql = text("CREATE TABLE IF NOT EXISTS test_table ("
                       "id INTEGER PRIMARY KEY AUTOINCREMENT,"
                       "trade_date DATE NOT NULL,"
                       "name VARCHAR(255) NOT NULL,"
                       "age INT NOT NULL"
                       ")")
            conn.execute(sql)

    def _drop_test_table(self):
        with self.db.engine.begin() as conn:
            sql = text("DROP TABLE IF EXISTS test_table")
            conn.execute(sql)

    def test_create_index(self):
        self._create_test_table()
        self.db.create_index("test_table")
        self._drop_test_table()
        print("SQLite index creation test passed")

    def test_remove_db(self):
        path: str = os.path.join(os.path.dirname(__file__), "test.db")
        os.remove(path)

if __name__ == "__main__":
    unittest.main()
