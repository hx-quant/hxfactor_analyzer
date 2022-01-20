# -*- coding: utf-8 -*-
"""
Created on Mon Jan 10 16:18:51 2022

@author: Administrator
"""

from functools import partial

import numpy as np
import pandas as pd
from fastcache import lru_cache

from Analyzer.hxfactor_analyzer.hx_when import date2str
from Database.DB_excel import EDB

edb = EDB()


class DataApi(object):

    def __init__(self, price='close', fq='post',
                 industry='sw_l1', weight_method='avg'):
        """数据接口, 用于因子分析获取数据

        参数
        ----------
        price : 使用开盘价/收盘价计算收益 (请注意避免未来函数), 默认为 'close'
            - 'close': 使用当日收盘价和次日收盘价计算当日因子的远期收益
            - 'open' : 使用当日开盘价和次日开盘价计算当日因子的远期收益
        fq : 价格数据的复权方式, 默认为 'post'
            - 'post': 后复权
            - 'pre': 前复权
            - None: 不复权
        industry : 行业分类, 默认为 'sw_l1'
            - 'sw_l1': 申万一级行业
        weight_method : 计算各分位收益时, 每只股票权重, 默认为 'avg'
            - 'avg': 等权重
            - 'mktcap': 按总市值加权
            - 'ln_mktcap': 按总市值的对数加权
            - 'cmktcap': 按流通市值加权
            - 'ln_cmktcap': 按流通市值的对数加权


        使用示例
        ----------
        from jqfactor_analyzer import DataApi, FactorAnalyzer

        api = DataApi(fq='pre', industry='sw_l1', weight_method='ln_mktcap')
        api.auth('username', 'password')

        factor = FactorAnalyzer(factor_data,
                                price=api.get_prices,
                                groupby=api.get_groupby,
                                weights=api.get_weights)
        # 或者
        # factor = FactorAnalyzer(factor_data, **api.apis)


        方法列表
        ----------
        get_prices : 价格数据获取接口
          参数 :
            securities : 股票代码列表
            start_date : 开始日期
            end_date : 结束日期
            count : 交易日长度
            (start_date 和 count)
          返回值 :
            pd.DataFrame
            价格数据, columns 为股票代码, index 为日期

        get_groupby : 行业分类数据获取接口
          参数 :
            securities : 股票代码列表
            start_date : 开始日期
            end_date : 结束日期
          返回值 :
            dict
            行业分类, {股票代码 -> 行业分类名称}

        get_weights : 股票权重获取接口
          参数 :
            securities : 股票代码列表
            start_date : 开始日期
            end_date : 结束日期
          返回值 :
            pd.DataFrame
            权重数据, columns 为股票代码, index 为日期


        属性列表
        ----------
        apis : dict, {'prices': get_prices, 'groupby': get_groupby,
                      'weights': get_weights}

        """
        valid_price = ('close', 'open')
        if price in valid_price:
            self.price = price
        else:
            ValueError("invalid 'price' parameter, "
                       "should be one of %s" % str(valid_price))

        valid_fq = ('post', 'pre', None)
        if fq in valid_fq:
            self.fq = fq
        else:
            raise ValueError("invalid 'fq' parameter, "
                             "should be one of %s" % str(valid_fq))

        valid_industry = ('sw_l1', 'sw_l2', 'sw_l3', 'zjw')
        if industry in valid_industry:
            self.industry = industry
        else:
            raise ValueError("invalid 'industry' parameter, "
                             "should be one of %s" % str(valid_industry))

        valid_weight_method = ('avg', 'mktcap', 'ln_mktcap',
                               'cmktcap', 'ln_cmktcap')
        if weight_method in valid_weight_method:
            self.weight_method = weight_method
        else:
            raise ValueError("invalid 'weight_method' parameter, "
                             "should be one of %s" % str(valid_weight_method))


    @property
    def api(self):
        if not hasattr(self, "_api"):
            raise NotImplementedError('api not specified')
        return self._api
    
    @lru_cache(2)
    def _get_trade_days(self, start_date=None, end_date=None):
        all_trade_days = edb.get_trade_days()
        start_date = all_trade_days[0] if start_date is None else start_date
        end_date = all_trade_days[-1] if end_date is None else end_date
        start_index = all_trade_days.index(pd.to_datetime(start_date))
        end_index = all_trade_days.index(pd.to_datetime(end_date))
        return list(all_trade_days[start_index:end_index+1])
    
    def _get_price(self, securities, start_date=None, end_date=None, fields=None):
        from datetime import datetime
        now = datetime.now()
        prices = edb.get_market_data('stock_close')
        prices = prices.loc[start_date:end_date,securities]
        print("获取股价！耗时：",datetime.now()-now)
        return prices

    def get_prices(self, securities, start_date=None, end_date=None, period=None):
        if period is not None:
            trade_days = self._get_trade_days(start_date=end_date)
            if len(trade_days):
                end_date = trade_days[:period + 1][-1]
        p = self._get_price(securities=securities, start_date=start_date, 
                            end_date=end_date)
        return p
    
    def _get_industry(self, securities, start_date, end_date,
                      industry='sw_l1'):
        from datetime import datetime
        now = datetime.now()
        industries = edb.get_industries(industry)
        industries = industries.loc[start_date:end_date,securities]
        print("获取行业分类！耗时：",datetime.now()-now)
        return industries

    def get_groupby(self, securities, start_date, end_date):
        return self._get_industry(securities=securities,
                                  start_date=start_date, end_date=end_date,
                                  industry=self.industry)
    
    def _get_market_cap(self, securities, start_date=None, end_date=None, ln=False):
        from datetime import datetime
        now = datetime.now()
        market_cap = edb.get_market_data('market_cap')
        market_cap = market_cap.loc[start_date:end_date,securities] * (10**8)
        if ln:
            market_cap = np.log(market_cap)
        print("获取总市值！耗时：",datetime.now()-now)
        return market_cap
    
    def _get_circulating_market_cap(self, securities, start_date, end_date,
                                    ln=False):
        from datetime import datetime
        now = datetime.now()
        cmarket_cap = edb.get_market_data('circulating_market_cap')
        cmarket_cap = cmarket_cap.loc[start_date:end_date,securities] * (10**8)
        if ln:
            cmarket_cap = np.log(cmarket_cap)
        print("获取流通市值！耗时：",datetime.now()-now)
        return cmarket_cap

    def _get_average_weights(self, securities, start_date, end_date):
        return {sec: 1.0 for sec in securities}

    def get_weights(self, securities, start_date, end_date):
        start_date = date2str(start_date)
        end_date = date2str(end_date)

        if self.weight_method == 'avg':
            weight_api = self._get_average_weights
        elif self.weight_method == 'mktcap':
            weight_api = partial(self._get_market_cap, ln=False)
        elif self.weight_method == 'ln_mktcap':
            weight_api = partial(self._get_market_cap, ln=True)
        elif self.weight_method == 'cmktcap':
            weight_api = partial(self._get_circulating_market_cap, ln=False)
        elif self.weight_method == 'ln_cmktcap':
            weight_api = partial(self._get_circulating_market_cap, ln=True)
        else:
            raise ValueError('invalid weight_method')

        return weight_api(securities=securities, start_date=start_date,
                          end_date=end_date)

    @property
    def apis(self):
        return dict(prices=self.get_prices,
                    groupby=self.get_groupby,
                    weights=self.get_weights)