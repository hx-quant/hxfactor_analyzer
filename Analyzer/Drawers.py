# -*- coding: utf-8 -*-
"""
Created on Sun Jan  9 00:48:18 2022

@author: Hxgre

草稿，用于测试每种方法的效果
"""

# 载入函数库
import pandas as pd
from jqfactor_analyzer import jqfactor_analyzer as ja

# 获取 jqdatasdk 授权，输入用户名、密码，申请地址：http://t.cn/EINDOxE
# 聚宽官网及金融终端，使用方法参见：http://t.cn/EINcS4j
import jqdatasdk
jqdatasdk.auth('18487205898','Xx170016')

# 获取5日平均换手率因子2018-01-01到2018-12-31之间的数据（示例用从库中直接调取）
# 聚宽因子库数据获取方法在下方
from jqfactor_analyzer.jqfactor_analyzer.sample import VOL5
factor_data = VOL5

# 对因子进行分析
far = ja.analyze_factor(
    factor_data,  # factor_data 为因子值的 pandas.DataFrame
    quantiles=10,
    periods=(1, 10),
    industry='jq_l1',
    weight_method='mktcap',
    max_loss=0.1
)

far.create_full_tear_sheet(
    demeaned=False, group_adjust=False, by_group=False,
    turnover_periods=None, avgretplot=(5, 15), std_bar=False
)


'''
# 载入函数库
import pandas as pd

# 获取 jqdatasdk 授权，输入用户名、密码，申请地址：http://t.cn/EINDOxE
# 聚宽官网及金融终端，使用方法参见：http://t.cn/EINcS4j
import jqdatasdk
jqdatasdk.auth('18487205898','Xx170016')

# 获取5日平均换手率因子2018-01-01到2018-12-31之间的数据（示例用从库中直接调取）
# 聚宽因子库数据获取方法在下方
from jqfactor_analyzer.jqfactor_analyzer.sample import VOL5
factor_data = VOL5

from HX_Analyzer import hxfactor_analyzer as hxja
hxfar = hxja.analyze_factor(
    factor_data,  # factor_data 为因子值的 pandas.DataFrame
    quantiles=10,
    periods=(1, 10),
    industry='sw_l1',
    weight_method='cmktcap',
    max_loss=0.1
)
hxfar.create_full_tear_sheet(
    demeaned=False, group_adjust=False, by_group=False,
    turnover_periods=None, avgretplot=(5, 15), std_bar=False
)
'''