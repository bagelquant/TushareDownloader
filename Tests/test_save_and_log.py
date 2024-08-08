"""

Author: Yanzhong Huang
Email: bagelquant@gmail.com
"""

import json
import os
from unittest import TestCase

from TushareDownloader.database import MySQL
from TushareDownloader.save_and_log import create_log_table
from sqlalchemy import text


class TestSaverLogger(TestCase):

    def setUp(self):
        """setup a mysql engine"""
        with open(os.path.join(os.path.dirname(__file__), 'test_config.json')) as f:
            config = json.load(f)['database_config']
        self.db = MySQL(**config)
        self.engine = self.db.get_engine()

    def test_create_log(self):
        create_log_table(self.engine)
        with self.engine.begin() as con:
            sql = text('SHOW TABLES')
            result = con.execute(sql)
            self.assertIn(('log',), [row for row in result])

            # clean up
            con.execute(text('DROP TABLE if EXISTS log'))
