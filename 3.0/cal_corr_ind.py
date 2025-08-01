from cal_return import get_complete_return

from all_cal import prerequisite,all_cal

import datetime as dt

for freq in ["5min","30min","12h"]:
    df_constant,composites,workday_list=prerequisite(index_code="SH000300",start=dt.datetime(2020,1,1),end=dt.datetime(2025,6,30),freq=freq)
    df_constant.corr().to_csv(f"corr_ind_{freq}.csv")






