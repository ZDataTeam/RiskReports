#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import xlwings as xw
from sqlalchemy import create_engine

import config

# 首次逾期
def 首期逾期():
    pass

#%%
if __name__=='__main__':

    # 连接数据库
    engine_oracle = create_engine(config.ConfigDevelopment.DB_ORACLE['str'], 
                                  connect_args={'encoding':'utf8', 'nencoding':'utf8'})
    engine_mysql = create_engine(config.ConfigDevelopment.DB_MYSQL['str'], 
                             connect_args={'charset':'utf8'})

    # 获取数据
    data1 = pd.read_sql("SELECT * FROM `TM_LOAN` WHERE LOAN_TYPE='MCEI' and PAID_OUT_DATE=0", engine_mysql)
    
    data2 = pd.read_sql("SELECT * FROM S6700 WHERE DATA_DT=TO_DATE(20180222, 'yyyymmdd')", engine_oracle)
    


