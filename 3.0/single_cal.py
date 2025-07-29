# 导入必要的数据分析和可视化库
import pandas as pd                       # 用于数据处理和分析
import numpy as np                        # 用于数值计算
import statsmodels.api as sm              # 用于统计建模（回归分析）
# from statsmodels.tools.sm_exceptions import MissingDataError  # 处理缺失数据异常

# import matplotlib.pyplot as plt          # 用于数据可视化
import datetime as dt                     # 用于日期时间处理

from cal_return import get_complete_return

import os

def log_error(msg,full_code):
    with open(f'error_list/{full_code}.txt', 'w', encoding='utf-8') as f:
        f.write(msg + '\n')

def parse_timedelta(time_str):
    """
    时间字符串解析函数

    将时间间隔字符串（如 "0.5D"、 "12H"、 "0.5min"）转换为 timedelta 对象。

    支持的时间单位：
    - W/week/weeks：周
    - D/day/days：天
    - H/hour/hours：小时
    - min/minute/minutes：分钟
    - S/sec/second/seconds：秒

    参数:
    time_str (str): 时间间隔字符串，如 "5min", "2H", "1D"等

    返回:
    datetime.timedelta: 对应的时间间隔对象
    """
    
    # 提取数值部分（从字符串开头提取数字和小数点）
    value_str = ""
    i = 0
    while i < len(time_str) and (time_str[i].isdigit() or time_str[i] == '.'):
        value_str += time_str[i]
        i += 1

    # Debug: 打印提取的数值部分
    # print(f"[DEBUG] Extracted value_str: '{value_str}'")

    if value_str == "":
        value = 1.0
    else:
        try:
            value = float(value_str)
        except Exception as e:
            # print(f"[DEBUG] Error converting value_str '{value_str}' to float: {e}")
            raise

    # 提取单位部分（去除空格并转为小写）
    unit = time_str[i:].strip().lower()
    # Debug: 打印提取的单位部分
    # print(f"[DEBUG] Extracted unit: '{unit}'")

    # 根据单位转换为对应的 timedelta 对象
    if unit in ['w', 'week', 'weeks']:  # 
        return dt.timedelta(weeks=value)
    if unit in ['d', 'day', 'days']:  # 天
        return dt.timedelta(days=value)
    elif unit in ['h', 'hour', 'hours']:  # 小时
        return dt.timedelta(hours=value)
    elif unit in ['min', 'minute', 'minutes']:  # 分钟
        return dt.timedelta(minutes=value)
    elif unit in ['s', 'sec', 'second', 'seconds']:  # 秒
        return dt.timedelta(seconds=value)
    else:
        # 不支持的时间单位，抛出异常
        raise ValueError(f"Unsupported period: {time_str}")

def simple_cal(df_stock,df_index,df_industry,full_code):
    # 无法回归
    if df_index.shape[0]<=1: return np.nan,np.nan,np.nan
    
    # 数据对齐


    if df_stock.shape[0]<df_index.shape[0] or df_stock.shape[0]<df_industry.shape[0]:
        log_error(f'{full_code} 数据长度不一致,start:{df_index.index[0]},end:{df_index.index[-1]}',full_code)
        log_error(f'stock_length:{df_stock.shape[0]},index_length:{df_index.shape[0]},industry_length:{df_industry.shape[0]}',full_code)
        missing_set=set(df_index.index.values)-set(df_stock.index.values)
        if len(missing_set)<3:
            log_error(f'缺少的index为{missing_set}',full_code)
        df_stock.to_csv(f'error_list/return/{full_code}.csv')
        if df_stock.shape[0]<=1: return np.nan,np.nan,np.nan
        df_index=df_index.loc[df_stock.index].copy()
        df_industry=df_industry.loc[df_stock.index].copy() 
    elif df_stock.shape[0]>df_index.shape[0] or df_stock.shape[0]>df_industry.shape[0]:
        log_error(f'{full_code} 数据长度不一致,stock_length:{df_stock.shape[0]},index_length:{df_index.shape[0]},industry_length:{df_industry.shape[0]}',full_code)
        missing_set=set(df_index.index.values)-set(df_stock.index.values)
        if len(missing_set)<3:
            log_error(f'缺少的index为{missing_set}',full_code)
        df_index=df_index.loc[df_stock.index].copy()
        df_industry=df_industry.loc[df_stock.index].copy()

    Y=df_stock
    X=pd.concat([df_index,df_industry],axis=1)
    X=sm.add_constant(X)
    model=sm.OLS(Y,X)
    results=model.fit()
    r2=results.rsquared
    params=results.params
    # try:
    beta_index=params.iloc[1]
    beta_industry=params.iloc[2]
    # except IndexError:
        # print(results.summary())
    return r2,beta_index,beta_industry

def simple_cross_section_cal(df_stock,df_index,df_industry,full_code,total_num):
    r2s=pd.Series()
    beta_indexs=pd.Series()
    beta_industrys=pd.Series()
    

    for nth in range(total_num):
        # for df in [df_stock_period,df_index_period,df_industry_period]:
            # 选取每天的第x个元素（例如第一个元素）
            # 这里以选取每天的第一个元素为例
        df_stock_nth= df_stock.groupby(df_stock.index.date).nth(nth)
        df_index_nth= df_index.groupby(df_index.index.date).nth(nth)
        df_industry_nth= df_industry.groupby(df_industry.index.date).nth(nth)
        # df_stock_period.to_csv(f'temp/{period_start.date()}_{period_end.date()}.csv')
        
        r2,beta_index,beta_industry=simple_cal(df_stock_nth,df_index_nth,df_industry_nth,full_code)
        r2s[nth]=r2
        beta_indexs[nth]=beta_index
        beta_industrys[nth]=beta_industry
    # r2s,beta_indexs,beta_industrys=simple_cal(full_code,df_index,df_industry,start,end,freq,workday_list,period,method="cross_section")
    return r2s,beta_indexs,beta_industrys

def single_periodic_cal(full_code,df_index,df_industry,start,end,freq,workday_list,period:int,method):
    ''''
        如果method=="

    '''
    df_stock,_,error_list=get_complete_return(full_code,start,end,freq,workday_list,False)
    # df_stock.to_csv('temp_stock.csv')

    if method=="simple":
        results=pd.DataFrame()
        if period=="full":
            simple_cal_result=simple_cal(df_stock,df_index,df_industry,full_code)
            results.loc[start,'r2']=simple_cal_result[0]
            results.loc[start,'beta_index']=simple_cal_result[1]
            results.loc[start,'beta_industry']=simple_cal_result[2]
            return results
        

        period=int(period)
        for period_start,period_end in zip(workday_list[::period],workday_list[period::period]):
            # 对于pd.date_range(start,end) 包含start和end 因此在选择的时候需要不包含end中的日期
            # period_end=dt.datetime.strptime(str(period_start),"%Y-%m-%d %H:%M:%S")+dt.timedelta(days=period)-dt.timedelta(seconds=1)
            # period_end=dt.datetime.strftime(period_end,"%Y-%m-%d %H:%M:%S")
            # print(period_start,period_end)
            df_index_period=df_index.loc[period_start:period_end]
            #对于选取1天的情况，可能出现的问题
            if df_index_period.shape[0]==0: continue
            df_industry_period=df_industry.loc[period_start:period_end]
            df_stock_period=df_stock.loc[period_start:period_end]
            
            # df_stock_period.to_csv(f'temp/{period_start.date()}_{period_end.date()}.csv')
            
            r2,beta_index,beta_industry=simple_cal(df_stock_period,df_index_period,df_industry_period,full_code)
            results.loc[period_start,'r2']=r2
            results.loc[period_start,'beta_index']=beta_index
            results.loc[period_start,'beta_industry']=beta_industry
        return results
    
    elif method=="cross_section":
        freq_num = int(''.join(filter(str.isdigit, str(freq))))
        total_num = 240 // freq_num +1

        r2s_all=pd.DataFrame(columns=range(total_num))
        beta_indexs_all=pd.DataFrame(columns=range(total_num))
        beta_industrys_all=pd.DataFrame(columns=range(total_num))
        if period=="full":
            r2s,beta_indexs,beta_industrys=simple_cross_section_cal(df_stock,df_index,df_industry,full_code,total_num)
            r2s=pd.DataFrame(r2s.rename(start)).T
            beta_indexs=pd.DataFrame(beta_indexs.rename(start)).T
            beta_industrys=pd.DataFrame(beta_industrys.rename(start)).T
            return r2s,beta_indexs,beta_industrys
        

        period=int(period)

        for period_start,period_end in zip(workday_list[::period],workday_list[period::period]):
            
            # 对于pd.date_range(start,end) 包含start和end 因此在选择的时候需要不包含end中的日期
            # period_end=dt.datetime.strptime(str(period_start),"%Y-%m-%d %H:%M:%S")+parse_timedelta(period)-dt.timedelta(seconds=1)
            # period_end=dt.datetime.strftime(period_end,"%Y-%m-%d %H:%M:%S")
            # print(period_start,period_end)
            df_index_period=df_index.loc[period_start:period_end]
            #对于选取1天的情况，可能出现的问题
            if df_index_period.shape[0]==0: continue
            df_industry_period=df_industry.loc[period_start:period_end]
            df_stock_period=df_stock.loc[period_start:period_end]
            r2s,beta_indexs,beta_industrys=simple_cross_section_cal(df_stock_period,df_index_period,df_industry_period,full_code,total_num)
            r2s_all.loc[period_start]=r2s
            beta_indexs_all.loc[period_start]=beta_indexs
            beta_industrys_all.loc[period_start]=beta_industrys


        return r2s_all,beta_indexs_all,beta_industrys_all

if __name__=="__main__":
    start=dt.datetime(2024,1,1)
    end=dt.datetime(2024,12,31)
    freq="5min"
    df_index,workday_list,_=get_complete_return(full_code="SH000300",start=start,end=end,freq=freq,workday_list=None,is_index=True)
    # print(workday_list)
    df_industry,_,error_list=get_complete_return(full_code="SH000070",start=start,end=end,freq=freq,workday_list=workday_list,is_index=True)

    method="simple"
    # for period in ["full","10"]:
    period="90"
    # for full_code in [""]
    full_code="SZ002352"
    print(full_code)
    # results=single_periodic_cal(full_code=full_code,df_index=df_index,df_industry=df_industry,start=start,end=end,freq=freq,workday_list=workday_list,period=period,method=method)
    # results.to_csv(f'test_results_{period}_{method}.csv')
        
    method="cross_section"
    # for period in ["full","10d"]:
    results=single_periodic_cal(full_code="SH600027",df_index=df_index,df_industry=df_industry,start=start,end=end,freq=freq,workday_list=workday_list,period=period,method=method)
    results[0].to_csv(f'test_results_{period}_{method}.csv')