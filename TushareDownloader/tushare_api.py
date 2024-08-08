"""
Tushare API

Author: Yanzhong Huang
Email: bagelquant@gmail.com

This module is wrapper around tushare api
"""

from tushare import pro_api
from tushare.pro.client import DataApi
from pandas import DataFrame


class TushareAPI:
    """
    A wrapper around tushare api

    - token: tushare token

    Methods:
        - download: download data from tushare
    """

    def __init__(self, token: str):
        self.token = token
        self.pro: DataApi = pro_api(token)

    def download(self, api_name: str, **kwargs) -> DataFrame:
        """
        Download data from tushare.pro

        :param api_name: tushare api name
        :param kwargs: arguments for tushare api
        """
        return self.pro.query(api_name, **kwargs)
