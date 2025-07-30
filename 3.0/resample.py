import pandas as pd
import numpy as np
import datetime as dt


def resample(df,freq,is_index,stock_code,workday_list,error_list,**kwargs):
    """
    股票高频数据重采样函数
    
    将输入的DataFrame按照指定频率重采样，同时处理中国股市的特殊交易时段
    包括早盘集合竞价、连续交易时段和收盘价
    
    参数:
    df (pd.DataFrame): 需要重采样的DataFrame，以Time (format:datetime) 为index,Price列位
    freq (str): 重采样频率，如"3min", "5min"等
    
    返回:
    pd.DataFrame: 重采样后的DataFrame
    """

    df_stock = df.copy()
    # df_stock[freq] = df_stock.index.time

    
    # 处理收盘价（15:00）
    # 按日取最后一个价格作为收盘价
    df_close = df_stock.resample('D').last()
    df_close=df_close.ffill()
    

    #处理关键时间段数据first(除权后的初始数据)
    if is_index:
        df_first = df_stock.resample('D').first()
        
        df_open = df_stock[df_stock.index.time<pd.to_datetime("09:25:59").time()].resample('D').last()
    
    else:
        # 个股进行分红处理
        df_dividend=pd.read_csv(f'dividend/{stock_code}.csv')
        df_dividend.index=pd.to_datetime(df_dividend['Exdistdt'])
        df_dividend=df_dividend.fillna(0)
        df_first=df_close[['Price']].shift(1)
        # print(df_first)
        
        start=df_close.index[0]
        end=df_close.index[-1]
        ex_dates=df_dividend[(df_dividend.index>=start)&(df_dividend.index<=end)].index.unique().tolist()
        # print(ex_dates)
        for date in ex_dates:
            # print(date)
            # print(df_first.loc[date,'Price'])
            df_first.loc[date,'Price']=(df_first.loc[date,'Price']-(df_dividend.loc[date,'Btperdiv']).sum())/(1+df_dividend.loc[date,'Perspt'].sum()+df_dividend.loc[date,'Pertran'].sum())
            # print(df_dividend.loc[date,'Btperdiv'].sum())
            # print(df_dividend.loc[date,'Perspt'].sum())
            # print(df_dividend.loc[date,'Pertran'].sum())
            # print(df_first.loc[date,'Price'])

        df_open = df_stock[df_stock.index.time<pd.to_datetime("09:25:59").time()].resample('D').last()


    df_first.index = df_first.index + dt.timedelta(hours=9) + dt.timedelta(minutes=15)
    df_open.index= df_open.index + dt.timedelta(hours=9) + dt.timedelta(minutes=25)
    df_close.index = df_close.index + dt.timedelta(hours=15)
    # 处理连续交易时段的数据
    # 按指定频率重采样，取每个时间窗口的最后一个价格
    df_continuous = df_stock.resample(freq).last()
    df_continuous = df_continuous.ffill()
    

    # 筛选连续交易时段：
    # 上午: 9:30-11:30
    # 下午: 13:00-14:59 (避开收盘集合竞价14:57-15:00
    # 时间选择11:29:59会导致一部分微小的信息丢失问题
    df_continuous = df_continuous[((df_continuous.index.time >= pd.to_datetime("09:30:00").time()) & 
                                (df_continuous.index.time < pd.to_datetime("11:29:59").time())) |
                                  ((df_continuous.index.time >= pd.to_datetime("13:00:00").time()) & 
                                   (df_continuous.index.time < pd.to_datetime("14:59:59").time()))]
    
    # 丢弃每一天最后一个df_continuous,用close替代
    last_idx_per_day = df_continuous.groupby(df_continuous.index.date).tail(1).index
    df_continuous = df_continuous.drop(last_idx_per_day)
    # 合并所有时段的数据
    df_resampled = pd.concat([df_first,df_open,df_continuous,df_close])
    df_resampled.sort_index(inplace=True)

    df_date_index = df_resampled.index[df_resampled.index.map(lambda x: x.date() in workday_list and x.date() not in error_list)]
    df_resampled=df_resampled[df_resampled.index.isin(df_date_index)]
    df_resampled.loc[df_resampled['Price']<0.00001,'Price']=np.nan
    df_resampled.ffill(inplace=True)
    df_resampled.index=pd.to_datetime(df_resampled.index)
    # df_stock=df_stock[df_stock[freq].isin(t_range.time)]  # 原有过滤逻辑，已注释
    
    # 数据清洗：处理集合竞价未形成价格的情况
    # 该数据库对于个股而言，如果集合竞价没有形成价格，Price填充为0
    # 对于填充为0的Price，将其设为NaN以便后续前向填充
    # if (np.abs(df_resampled['Price'] - 0) < 0.000001).any():
    #     print(df_resampled[np.abs(df_resampled['Price'] - 0) < 0.000001])
    #     df_resampled.loc[np.abs(df_resampled['Price'] - 0) < 0.000001, 'Price'] = np.nan
    
    # 对于没有形成开盘价的股票和freq内没有交易的股票，前向填充前一次交易的价格
    # df_resampled['Price'] = df_resampled['Price'].ffill()
    


    return df_resampled
if __name__=="__main__":
    from get_data import get_data
    df_all=get_data(start=dt.datetime(2024,9,24),end=dt.datetime(2024,10,10),exg="SZ",full_code="SZ002352")
    df_r=resample(df_all[0],freq="10min",is_index=False,stock_code="002352",workday_list=df_all[1],error_list=df_all[2])
    df_r.to_csv('resample_test.csv')