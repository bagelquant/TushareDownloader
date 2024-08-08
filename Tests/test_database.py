"""
Tests for mysql database

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

import json
import os
from unittest import TestCase

from TushareDownloader.database import MySQL
from sqlalchemy import text


class TestMySQLDB(TestCase):

    def setUp(self):
        """initialize mysql db"""
        with open(os.path.join(os.path.dirname(__file__), 'test_config.json')) as f:
            config = json.load(f)['database_config']

        self.db = MySQL(**config)

    def test_conenction(self):
        with self.db.engine.begin() as conn:
            sql = text('SHOW TABLES')
            result = conn.execute(sql)
            print([row for row in result])

    def test_create_index(self):
        self.db.create_index('stock_basic')
