from single_cal import single_periodic_cal
from cal_return import get_complete_return

import pandas as pd 
import datetime as dt
import numpy as np

def get_composites(index_code):
    composites = pd.DataFrame()
    composites['full_code'] = pd.read_excel(f'index/{index_code}.xlsx')['证券代码']
    composites.drop(index=composites.index[-2:], inplace=True)
    composites['exg'] = composites['full_code'].str.split('.').str[1]      # 提取交易所代码
    composites['num_code'] = composites['full_code'].str.split('.').str[0]     # 提取股票代码
    composites['full_code'] = composites['exg'] + composites['num_code']   
    # composites.set_index('full_code',inplace=True)    # 重新组合为统一格式
    return composites

def prerequisite(index_code,start,end,freq):
    df_index,workday_list,_=get_complete_return(full_code="SH000300",start=start,end=end,freq=freq,workday_list=None,is_index=True)
    # print(workday_list)
    df_constant=df_index.rename(index_code)
    industry_matching=pd.read_csv("industry/stock_index_match.csv")
    industry_code_list=industry_matching['index'].unique().tolist()
    for industry_code in industry_code_list: 
        df_industry,_,error_list=get_complete_return(full_code=industry_code,start=start,end=end,freq=freq,workday_list=workday_list,is_index=True)

        df_constant=pd.concat([df_constant,df_industry.rename(industry_code)],axis=1)
    
    composites=get_composites(index_code)
    composites=composites.merge(industry_matching,on='full_code',how='left')
    
    return df_constant,composites,workday_list

def all_cal(index_code,df_constant,composites,start,end,freq,workday_list,period,method):
    composite_list=composites['full_code'].tolist()
    final_r2_result={}
    final_r2_index_result={}
    final_r2_industry_result={}
    processing_count=0
    for full_code in composite_list:
        industry_code=composites.loc[composites['full_code']==full_code,'index'].values[0]
        if method=="simple":
            results=single_periodic_cal(full_code=full_code,df_index=df_constant[index_code],df_industry=df_constant[industry_code],start=start,end=end,freq=freq,workday_list=workday_list,period=period,method=method)
            results.to_csv(f"temp/{full_code}.csv")
            final_r2_result[full_code]=results

        elif method=="cross_section":
            df_r2,df_beta_index,df_beta_industry=single_periodic_cal(full_code=full_code,df_index=df_constant[index_code],df_industry=df_constant[industry_code],start=start,end=end,freq=freq,workday_list=workday_list,period=period,method=method)
            df_r2.to_csv(f"temp/{full_code}.csv")
            final_r2_result[full_code]=df_r2
        else:
            raise ValueError(f"method {method} not supported")
        
        processing_count+=1
        # if processing_count%10==5: 
        print(full_code)
            # break
    summed_df = sum_df(final_r2_result)

    return summed_df

def sum_df(result_dict):
    summed_df = None
    for key,df in result_dict.items():
        if summed_df is None:
            summed_df = df.copy()
        else:
            summed_df = summed_df.add(df, fill_value=0)
    return summed_df

def continue_cal(index_code,df_constant,composites,start,end,freq,workday_list,period,method,continue_code):
    # rangeIndex无法slide index
    continue_index=composites[composites['full_code']==continue_code].index[0]
    composites_continue=composites[continue_index:].copy()
    # print(composites_continue)
    all_cal(index_code=index_code,df_constant=df_constant,composites=composites_continue,start=start,end=end,freq=freq,workday_list=workday_list,period=period,method=method)
    results_dict={}
    for full_code in composites["full_code"].tolist():
        results=pd.read_csv(f"temp/{full_code}.csv")
        results_dict[full_code]=results
    return sum_df(results_dict)     

if __name__=="__main__":
    start=dt.datetime(2024,1,1)
    end=dt.datetime(2024,12,31)
    freq="5min"
    df_constant,composites,workday_list=prerequisite(index_code="SH000300",start=start,end=end,freq=freq)
    # print(composites)
    # results=all_cal(index_code="SH000300",df_constant=df_constant,composites=composites,start=start,end=end,freq=freq,workday_list=workday_list,period="10",method="simple")
    # results.to_csv('test_results_all.csv')

    results=continue_cal(index_code="SH000300",df_constant=df_constant,composites=composites,start=start,end=end,freq=freq,workday_list=workday_list,period="10",method="simple",continue_code="SZ002252")
    results.to_csv('test_results_continue.csv')