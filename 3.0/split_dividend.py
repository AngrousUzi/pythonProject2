import pandas as pd

df_dividend=pd.read_excel('dividend/CD_dividend.xlsx')
df_dividend.drop(index=df_dividend.index[0:2],inplace=True)
df_dividend=df_dividend[['Stkcd','Exdistdt','Perspt','Pertran','Btperdiv']]
df_dividend

code_list=df_dividend['Stkcd'].unique()
for code in code_list:
    df_tmp=df_dividend[df_dividend['Stkcd']==code]
    df_tmp=df_tmp[['Exdistdt','Perspt','Pertran','Btperdiv']]
    df_tmp=df_tmp[~df_tmp.isnull().all(axis=1)]
    df_tmp.to_csv(f'dividend/{code}.csv',index=False)
