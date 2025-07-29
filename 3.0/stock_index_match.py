import pandas as pd

def convert_to_exchange_code(stock_code):
    """
    将股票代码转换为交易所代码格式
    深圳交易所: 000、002、003 开头 → SZ + 代码
    上海交易所: 600、601、603、605、688 开头 → SH + 代码
    """
    if pd.isna(stock_code):
        return stock_code
    
    # 转换为字符串并确保6位数字格式
    code_str = str(stock_code)
    if code_str.endswith('HK'):
        return code_str    
    # 判断交易所
    if code_str.startswith(('000', '001','002', '003','004','30')):
        return f'SZ{code_str}'
    elif code_str.startswith(('600', '601', '603', '605', '688')):
        return f'SH{code_str}'


stock_ind_match = pd.read_excel('industry/行业分类.xlsx') 
stock_ind_match.rename(columns={'中证一级行业分类简称':'industry','证券代码':"num_code"}, inplace=True)

# 添加交易所代码列
stock_ind_match['full_code'] = stock_ind_match['num_code'].apply(convert_to_exchange_code)

ind_index_match = pd.read_excel('industry/industry.xlsx')
ind_index_match['index'] = ind_index_match['index'].str.split('.').str[1] + ind_index_match['index'].str.split('.').str[0]
stock_index_match = pd.merge(stock_ind_match, ind_index_match, on="industry", how='left')
stock_index_match = stock_index_match[["industry", "num_code", "full_code", "index"]]
stock_index_match.to_csv('industry/stock_index_match.csv', encoding='utf-8-sig', index=False)










