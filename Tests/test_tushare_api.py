"""

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

import json
import os
from unittest import TestCase

from TushareDownloader.tushare_api import TushareAPI


class TestTushareAPI(TestCase):

    def setUp(self):
        """set up token"""
        with open(os.path.join(os.path.dirname(__file__), 'test_config.json')) as f:
            self.token = json.load(f)['tushare_token']
        self.tushare_api = TushareAPI(self.token)

    def test_download(self):
        api_name: str = 'stock_basic'
        kwargs = {'exchange': '',
                  'list_status': 'L',
                  'fields': ['ts_code', 'symbol', 'name', 'area', 'industry', 'list_date']}

        df = self.tushare_api.download(api_name, **kwargs)
        self.assertIsNotNone(df)
        self.assertEqual(df.shape[1], 6)
        self.assertGreater(df.shape[0], 0)
        print(df.head())
