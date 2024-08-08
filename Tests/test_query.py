"""

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""
import os
import json
from unittest import TestCase

from TushareDownloader.query import Query
from TushareDownloader import MySQL
from datetime import datetime


class TestQuery(TestCase):

    def setUp(self):
        """setup database and query object for test"""
        with open(os.path.join(os.path.dirname(__file__), 'test_config.json')) as f:
            config = json.load(f)['database_config']
        self.db = MySQL(**config)
        self.query = Query(self.db.get_engine())

    def test_stock_list(self):
        print(self.query.stock_list())

    def test_trade_cal(self):
        print(self.query.trade_cal(datetime(2021, 1, 1), datetime(2021, 1, 31)))

    def test_end_date_in_log(self):
        print(self.query.end_date_in_log('stock_basic', 'all'))
