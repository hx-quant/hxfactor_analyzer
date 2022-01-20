# -*- coding: utf-8 -*-
"""
Created on Wed Jan  5 15:52:22 2022

@author: Administrator
"""
from Database.DB_excel import EDB
#from Tools import myTools

import jqdatasdk as jq
import datetime
import pandas as pd
import numpy as np

edb = EDB()

START_DATE = '2010-01-01'
END_DATE = '2020-12-31'

FILE = "E:\workspace_py\Database\Data_EXCEL\\"

def _init():
    jq.auth('18487205898','Xx170016')
    return None

def query_counts():
    count = jq.get_query_count()
    print("时间：",datetime.datetime.now(),"总数量：",count['total'],"剩余数量：",count['spare'])
    return None

def all_stock_info():
    """
    股票基本信息
    Index     证券代码     证券简称     英文代码     上市日期     退市日期
    1         000001.XSHE  平安银行     PAYH        1991-04-03   NaN

    """
    all_stock_info = jq.get_all_securities(types=['stock'],date=None)
    #证券名称
    names = all_stock_info[['display_name','name']]
    #上市日期
    ipo_date = pd.Series([x.strftime('%Y-%m-%d') for x in all_stock_info['start_date']],\
                         index=list(all_stock_info.index))
    #退市日期：尚未退市则为NaN
    end_date = pd.Series(index=list(all_stock_info.index))
    items = all_stock_info['end_date']
    for i in range(0,len(items)):
        code = items.index[i]
        if items[code] == pd.to_datetime('2200-01-01'):
            end_date[code] = np.NaN
        else:
            end_date[code] = items[code].strftime('%Y-%m-%d')
    result = pd.concat([names,ipo_date,end_date],axis=1)
    result = result.reset_index()
    result.columns = ['证券代码','证券简称','英文代码','上市日期','退市日期']
    
    file = FILE + 'Information\securities_information.xlsx'
    edb.update(file,result)
    return None

def trade_days():
    """
    以上证指数的交易日期为准
    """
    file = FILE + 'Market_Data/index_close.xlsx'
    trade_days = edb.get_index(file)
    file = FILE + 'Market_Data/trade_days.xlsx'
    edb.update(file,trade_days)
    return trade_days

def index_daily_market_data():
    """
    指数日行情数据，分为：CLOSE/OPEN/HIGH/LOW/VOLUME/MONEY
    指数共分为以下几种：
    * 上证指数   000001.XSHG
    * 中证1000   000852.XSHG
    * 中证500    000905.XSHG
    * 深证成指   399001.XSHE
    * 创业板指   399006.XSHE
    * 沪深300   399300.XSHE
    """
    #上证指数
    old_c = jq.get_query_count()['spare']
    szzs = jq.get_price('000001.XSHG', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    #中证1000
    old_c = jq.get_query_count()['spare']
    zz1000 = jq.get_price('000852.XSHG', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    #中证500
    old_c = jq.get_query_count()['spare']
    zz500 = jq.get_price('000905.XSHG', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    #深证成指
    old_c = jq.get_query_count()['spare']
    szcz = jq.get_price('399001.XSHE', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    #创业板指
    old_c = jq.get_query_count()['spare']
    cybz = jq.get_price('399006.XSHE', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    #沪深300
    old_c = jq.get_query_count()['spare']
    hs300 = jq.get_price('399300.XSHE', start_date=START_DATE, end_date=END_DATE,\
                        frequency='daily', fields=None, skip_paused=False, fq='pre', panel=False)
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    
    #整合数据
    #收盘价
    idx_close = pd.concat([szzs['close'],zz1000['close'],zz500['close'],szcz['close'],\
                           cybz['close'],hs300['close']],axis=1)
    idx_close.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_close.xlsx'
    edb.update(file,idx_close)
    #开盘价
    idx_open = pd.concat([szzs['open'],zz1000['open'],zz500['open'],szcz['open'],\
                           cybz['open'],hs300['open']],axis=1)
    idx_open.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_open.xlsx'
    edb.update(file,idx_open)
    #最高价
    idx_high = pd.concat([szzs['high'],zz1000['high'],zz500['high'],szcz['high'],\
                           cybz['high'],hs300['high']],axis=1)
    idx_high.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_high.xlsx'
    edb.update(file,idx_high)
    #最低价
    idx_low = pd.concat([szzs['low'],zz1000['low'],zz500['low'],szcz['low'],\
                           cybz['low'],hs300['low']],axis=1)
    idx_low.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_low.xlsx'
    edb.update(file,idx_low)
    #成交量
    idx_volume = pd.concat([szzs['volume'],zz1000['volume'],zz500['volume'],szcz['volume'],\
                           cybz['volume'],hs300['volume']],axis=1)
    idx_volume.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_volume.xlsx'
    edb.update(file,idx_volume)
    #成交额
    idx_money = pd.concat([szzs['money'],zz1000['money'],zz500['money'],szcz['money'],\
                           cybz['money'],hs300['money']],axis=1)
    idx_money.columns = ['上证指数','中证1000','中证500','深证成指','创业板指','沪深300']
    file = FILE + 'Market_Data\index_money.xlsx'
    edb.update(file,idx_money)
    return None

def _check_index(old_index,new_index):
    dup_index = set(old_index).intersection(set(new_index))
    exc_dup_index = list(set(new_index) - dup_index)
    exc_dup_index.sort()
    return exc_dup_index

def _check_columns(old_columns,new_columns):
    add_columns = list(set(new_columns) - set(old_columns).intersection(set(new_columns)))
    add_columns.sort()
    old_columns.extend(add_columns)
    new_columns = old_columns
    return new_columns

def stock_infos():
    """
    存储已有market_data数据的行列名，以便后续查看
    """
    #提取数据库已有日期
    file = FILE + 'Market_Data\stock_open.xlsx'
    ind_names = edb.get_index(file)
    col_names = edb.get_columns(file)
    
    file = FILE + 'Market_Data\stock_index.xlsx'
    edb.update(file,ind_names)
    file = FILE + 'Market_Data\stock_columns.xlsx'
    edb.update(file,col_names)
    return None

def stock_market_data():
    #提取数据库已有上市公司相关信息
    file = FILE + 'Information\securities_information.xlsx'
    stock_infos = pd.read_excel(file,index_col=0)
    ipo_dates = stock_infos['上市日期']
    ipo_dates.index = stock_infos['证券代码']
    end_dates = stock_infos['退市日期']
    end_dates.index = stock_infos['证券代码']
    old_symbol = list(stock_infos['证券代码'].values)
    #提取交易日
    file = FILE + 'Market_Data\index_close.xlsx'
    idx_close = pd.read_excel(file,index_col=0)
    trade_days = list(idx_close.index)
    #提取数据库股票数据已有交易日期和证券代码
    file = FILE + 'Market_Data\stock_index.xlsx'
    old_index = list(edb.get_index(file))
    file = FILE + 'Market_Data\stock_columns.xlsx'
    old_columns = list(edb.get_index(file))
    
    #确定新增股票
    #symbol = list(set(old_symbol) + set(open_columns) + set(close_columns) + set(high_columns)\
    #        + set(low_columns) + set(volume_columns) + set(money_columns))
    symbol = old_symbol
    
    #确定本次要提取范围：交易日
    for i in range(0,len(trade_days)):
        if pd.to_datetime(trade_days[i]) >= pd.to_datetime(START_DATE):
            traDay_start = i
            break
    for j in range(0,len(trade_days)):
        if pd.to_datetime(trade_days[j]) > pd.to_datetime(END_DATE):
            traDay_end = j
            break
        traDay_end = j + 1
    extract_traDays = trade_days[traDay_start:traDay_end]
    
    #利用JoinQuant提取数据
    new_open = pd.DataFrame(index=extract_traDays,columns=symbol)
    new_close = pd.DataFrame(index=extract_traDays,columns=symbol)
    new_high = pd.DataFrame(index=extract_traDays,columns=symbol)
    new_low = pd.DataFrame(index=extract_traDays,columns=symbol)
    new_volume = pd.DataFrame(index=extract_traDays,columns=symbol)
    new_money = pd.DataFrame(index=extract_traDays,columns=symbol)
    old_c = jq.get_query_count()['spare']
    for stock in symbol:
        ipo_date = ipo_dates[stock]
        end_date = end_dates[stock]
        #确定“开始日期”
        if pd.to_datetime(ipo_date) < pd.to_datetime(START_DATE):
            beg = START_DATE
        elif pd.to_datetime(ipo_date) <= pd.to_datetime(END_DATE):
            beg = ipo_date
        else:
            continue
        #确定“截止日期”
        if end_date is np.nan or pd.to_datetime(end_date) > pd.to_datetime(END_DATE):
            end = END_DATE
        elif pd.to_datetime(end_date) >= pd.to_datetime(START_DATE):
            end = end_date
        else:
            continue
        x_data = jq.get_price(stock,start_date=beg,end_date=end, frequency='daily',\
                              fields=None, skip_paused=False,fq='pre',panel=False)
        new_open.loc[x_data.index,stock] = x_data['open']
        new_close.loc[x_data.index,stock] = x_data['close']
        new_high.loc[x_data.index,stock] = x_data['high']
        new_low.loc[x_data.index,stock] = x_data['low']
        new_volume.loc[x_data.index,stock] = x_data['volume']
        new_money.loc[x_data.index,stock] = x_data['money']
    new_c = jq.get_query_count()['spare']
    print('本次共计提取数据数量：',old_c - new_c,"本日还剩数据量：",new_c)
    
    #检查列名和行名，确认待写入数据的非重复和顺序
    new_index = _check_index(old_index,new_open.index)
    new_columns = _check_columns(old_columns,new_open.columns)    
    new_open2 = new_open.loc[new_index,new_columns]  
    new_close2 = new_close.loc[new_index,new_columns]
    new_high2 = new_high.loc[new_index,new_columns]
    new_low2 = new_low.loc[new_index,new_columns]
    new_volume2 = new_volume.loc[new_index,new_columns]
    new_money2 = new_money.loc[new_index,new_columns]
    
    #数据存储
    beg_i = len(old_index)
    file = FILE + 'Market_Data\stock_open.xlsx'
    edb.update(file,new_open2,beg_idx=beg_i)
    file = FILE + 'Market_Data\stock_close.xlsx'
    edb.update(file,new_close2,beg_idx=beg_i)
    file = FILE + 'Market_Data\stock_high.xlsx'
    edb.update(file,new_high2,beg_idx=beg_i)
    file = FILE + 'Market_Data\stock_low.xlsx'
    edb.update(file,new_low2,beg_idx=beg_i)
    file = FILE + 'Market_Data\stock_volume.xlsx'
    edb.update(file,new_volume2,beg_idx=beg_i)
    file = FILE + 'Market_Data\stock_money.xlsx'
    edb.update(file,new_money2,beg_idx=beg_i)
    return None

def groupby():
    """
    分年度存储行业分类
                   sw_l1         sw_l2
    000001.XSHE    金融地产      房地产
    ...         ...          ...
    
    这里主要存储“申万一级”、“申万二级”、“三级”的行业分类数据
    """
    #提取证券代码
    file = FILE + 'Information\securities_information.xlsx'
    stock_infos = pd.read_excel(file,index_col=0)
    securities = list(stock_infos['证券代码'].values)
    
    industries = jq.get_industry(security=securities,date='2018-12-31')
    
    s = securities[0]
    industry = list(industries.get(s).keys())
    industries_df = pd.DataFrame(index=securities,columns=industry)
    for stock in securities:
        for name in industry:
            if name in list(industries.get(stock).keys()):
                industries_df.loc[stock,name] = industries.get(stock).get(name).get('industry_name')
                
    file = FILE + 'Information\industries_2018.xlsx'
    edb.update(file,industries_df)
    return None

def groupby2():
    """
    行业分类
                000001.XSHE   000002.XSHE
    2010.1.1    金融地产      房地产
    ...         ...          ...
    2020.12.31  金融地产      房地产
    
    这里主要存储“申万一级”、“申万二级”、“三级”的行业分类数据
    """
    #提取证券代码
    file = FILE + 'Information\securities_information.xlsx'
    stock_infos = pd.read_excel(file,index_col=0)
    symbol = list(stock_infos['证券代码'].values)
    ipo_dates = stock_infos['上市日期']
    ipo_dates.index = stock_infos['证券代码']
    #提取交易日
    file = FILE + 'Market_Data\index_close.xlsx'
    idx_close = pd.read_excel(file,index_col=0)
    trade_days = list(idx_close.index)
    
    #确定本次提取的股票池
    securities = list()
    for s in symbol:
        if pd.to_datetime(ipo_dates[s]) <= pd.to_datetime(END_DATE):
            securities.append(s)
    
    oldc = jq.get_query_count()['spare']
    from functools import partial
    industries = map(partial(jq.get_industry, symbol), extract_traDays)
    industry = 'sw_l1'
    industries = {
        d: {
            s: ind.get(s).get(industry, dict()).get('industry_name', 'NA')
            for s in symbol
        }
        for d, ind in zip(extract_traDays, industries)
    }
    newc = jq.get_query_count()['spare']
    print("本次消耗：",oldc-newc,"剩余数量：",newc)
    
    ind_df = pd.DataFrame(industries).T.sort_index()
    file = FILE + 'Information\industries.xlsx'
    edb.update(file,ind_df)    
    return None

def st():
    """
    ST记录

    """
    #提取数据库已有上市公司相关信息
    file = FILE + 'Information\securities_information.xlsx'
    stock_infos = pd.read_excel(file,index_col=0)
    symbol = list(stock_infos['证券代码'].values)
    
    st = jq.get_extras('is_st', symbol, start_date=START_DATE, end_date=END_DATE)
    
    file = FILE + 'Information\st.xlsx'
    edb.update(file,st)   
    return None

def valuation():
    """
    市值数据相关数据
    
    聚宽中相关的表头
    
    列名                          含义
    code   ------------------   股票代码
    day   -------------------   日期
    capitalization   --------   总股本（万股）
    circulating_cap   -------   流通股本（万股）
    market_cap   ------------   总市值（亿元）
    circulating_market_cap      流通市值（亿元）
    turnover_ratio   --------   换手率（%）
    pe_ratio   --------------   市盈率（PE，TTM）
    pe_ratio_lyr   ----------   市盈率（PE）
    pb_ratio   --------------   市净率（PB）
    ps_ratio   --------------   市销率（PS，TTM）
    pcf_ratio   -------------   市现率（PCF，现金净流量TTM）
    """
    #提取证券代码
    file = FILE + 'Information\securities_information.xlsx'
    stock_infos = pd.read_excel(file,index_col=0)
    symbol = list(stock_infos['证券代码'].values)
    #提取交易日
    file = FILE + 'Market_Data\index_close.xlsx'
    idx_close = pd.read_excel(file,index_col=0)
    trade_days = list(idx_close.index)
    
    #创建DF存储各类数据
    capitalization = np.zeros((len(trade_days),len(symbol)))
    circulating_cap = np.zeros((len(trade_days),len(symbol)))
    market_cap = np.zeros((len(trade_days),len(symbol)))
    circulating_market_cap = np.zeros((len(trade_days),len(symbol)))
    turnover_ratio = np.zeros((len(trade_days),len(symbol)))
    pe_ratio = np.zeros((len(trade_days),len(symbol)))
    pe_ratio_lyr = np.zeros((len(trade_days),len(symbol)))
    pb_ratio = np.zeros((len(trade_days),len(symbol)))
    ps_ratio = np.zeros((len(trade_days),len(symbol)))
    pcf_ratio = np.zeros((len(trade_days),len(symbol)))
    for date in trade_days:
        cur_date = date.strftime("%Y-%m-%d")
        print(cur_date)
        q = jq.query(jq.valuation).filter()
        df = jq.get_fundamentals(q,cur_date)
        df = df.set_index('code')
        """
        每一交易日的数据，形如下表：
        Index   id        code          pe_ratio   turnover_ratio   pb_ratio ...
        0       5024884   000001.XSHE   ...        ...              ...      ...
        """
        y_index = trade_days.index(date)
        for code in df.index:
            x_index = symbol.index(code)
            capitalization[y_index][x_index] = df.loc[code,'capitalization']
            circulating_cap[y_index][x_index] = df.loc[code,'circulating_cap']
            market_cap[y_index][x_index] = df.loc[code,'market_cap']
            circulating_market_cap[y_index][x_index] = df.loc[code,'circulating_market_cap']
            turnover_ratio[y_index][x_index] = df.loc[code,'turnover_ratio']
            pe_ratio[y_index][x_index] = df.loc[code,'pe_ratio']
            pe_ratio_lyr[y_index][x_index] = df.loc[code,'pe_ratio_lyr']
            pb_ratio[y_index][x_index] = df.loc[code,'pb_ratio']
            ps_ratio[y_index][x_index] = df.loc[code,'ps_ratio']
            pcf_ratio[y_index][x_index] = df.loc[code,'pcf_ratio']
            
    capitalization2 = pd.DataFrame(capitalization,index=trade_days,columns=symbol)
    circulating_cap2 = pd.DataFrame(circulating_cap,index=trade_days,columns=symbol)
    market_cap2 = pd.DataFrame(market_cap,index=trade_days,columns=symbol)
    circulating_market_cap2 = pd.DataFrame(circulating_market_cap,index=trade_days,columns=symbol)
    turnover_ratio2 = pd.DataFrame(turnover_ratio,index=trade_days,columns=symbol)
    pe_ratio2 = pd.DataFrame(pe_ratio,index=trade_days,columns=symbol)
    pe_ratio_lyr2 = pd.DataFrame(pe_ratio_lyr,index=trade_days,columns=symbol)
    pb_ratio2 = pd.DataFrame(pb_ratio,index=trade_days,columns=symbol)
    ps_ratio2 = pd.DataFrame(ps_ratio,index=trade_days,columns=symbol)
    pcf_ratio2 = pd.DataFrame(pcf_ratio,index=trade_days,columns=symbol)

    file = FILE + 'Market_Data\capitalization.xlsx'
    edb.update(file,capitalization2)
    file = FILE + 'Market_Data\circulating_cap.xlsx'
    edb.update(file,circulating_cap2)
    file = FILE + 'Market_Data\market_cap.xlsx'
    edb.update(file,market_cap2)
    file = FILE + 'Market_Data\circulating_market_cap.xlsx'
    edb.update(file,circulating_market_cap2)
    file = FILE + 'Market_Data\turnover_ratio.xlsx'
    edb.update(file,turnover_ratio2)
    file = FILE + 'Market_Data\pe_ratio.xlsx'
    edb.update(file,pe_ratio2)
    file = FILE + 'Market_Data\pe_ratio_lyr.xlsx'
    edb.update(file,pe_ratio_lyr2)
    file = FILE + 'Market_Data\pb_ratio.xlsx'
    edb.update(file,pb_ratio2)
    file = FILE + 'Market_Data\ps_ratio.xlsx'
    edb.update(file,ps_ratio2)
    file = FILE + 'Market_Data\pcf_ratio.xlsx'
    edb.update(file,pcf_ratio2)
    return None

def financial_data():
    return None

