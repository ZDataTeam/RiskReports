#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import xlwings as xw
from sqlalchemy import create_engine

import config

def _translate(df, dct_dimension, dct_col, is_dimension=False):
    """翻译字段"""

    temp = df.copy()
    
    if is_dimension:
        # 翻译数据值维度
        temp = temp.apply(lambda x:x.map(dct_dimension[x.name])
        if ((x.name in dct_dimension) and (9999 not in dct_dimension.get(x.name))) else x)
        
    else:
        # 翻译行标题和列标题
        [temp.rename(index=dct_dimension[x], level=x, inplace=True) for x in temp.index.names if x in dct_dimension]
        [temp.rename(columns=dct_dimension[x], level=x, inplace=True) for x in temp.columns.names if x in dct_dimension]
    
        # 翻译行名和列名
        temp.index.names = pd.Series(temp.index.names).map(lambda x: dct_col[x] if x in dct_col else x)
        temp.columns.names = pd.Series(temp.columns.names).map(lambda x: dct_col[x] if x in dct_col else x)

    return(temp)

def _patch(df, idx_dates=None):
    """插入数据透视表缺失月份列，转化为vintage"""
    temp = df.copy()
    
    # idx_dates是df的日期index列表
    if idx_dates is None:
        if isinstance(temp.index, pd.MultiIndex):
            idx_dates = temp.index.get_level_values(len(temp.index.levels)-1)
        else:
            idx_dates = temp.index
    
    # 插入新列
    for i, dt in enumerate(pd.date_range('20150801', pd.datetime.today().strftime('%Y%m%d'), freq='M')):
        if dt not in temp.columns:
            temp.insert(i, dt, idx_dates.map(lambda x: pd.np.nan if x > dt else 0))
    
    # 对角线空白补零
    try:
        for dt in temp.columns:
            if pd.isnull(temp.loc[dt, dt]):
                temp.loc[dt, dt] = 0
    except KeyError: # 忽略错误
        pass
    
    # 左移空白单元格
    temp = temp.apply(lambda x:x.shift(-x.index.get_loc(x.first_valid_index())), axis=1).rename(
            columns=lambda x: '第' + 
            str(pd.offsets.relativedelta(x, db_data.begin_date.min()).years * 12 + pd.offsets.relativedelta(x, db_data.begin_date.min()).months) + 
            '个月')
    
    return(temp)

def overdue(db_data, dct_dimension, dct_col, gp_keys_all, gp_keys_last, gp_keys_prov=None):
    """逾期不良表"""

    def od_template(data, gp_keys):
        """逾期不良模板表"""
        all_1 = data[gp_keys+['cnt','loan_pr','bal_prin','bal']].groupby(gp_keys).sum().rename(
                columns={'cnt':'放款人次', 'loan_pr':'放款总额', 'bal_prin':'贷款本金余额', 'bal':'贷款余额'})
        all_2 = data[(data.overdue_status_3 != 2)][gp_keys+['cnt']].groupby(gp_keys).sum().rename(
                columns={'cnt':'未结清户数'})
        all_3 = data[(data.new_loan == 1)][gp_keys+['cnt']].groupby(gp_keys).sum().rename(
                columns={'cnt':'月度新增'})
        all_4 = data[(data.overdue_status_3 == 1)][gp_keys+['cnt', 'od_principal', 'od_amt']].groupby(gp_keys).sum().rename(
                columns={'cnt':'本金逾期户数', 'od_principal':'本金逾期金额', 'od_amt':'逾期金额'})
        all_5 = data[(data.overdue_status_3 == 1) & (data.maturity_days > 0)][gp_keys+['cnt', 'od_principal', 'od_amt']].groupby(gp_keys).sum().rename(
                columns={'cnt':'银行逾期户数', 'od_principal':'本金银行逾期', 'od_amt':'银行逾期'})
        all_6 = data[(data.overdue_status_3 == 1) & (data.maturity_days > 2)][gp_keys+['cnt', 'od_principal', 'od_amt']].groupby(gp_keys).sum().rename(
                columns={'cnt':'不良逾期户数', 'od_principal':'本金不良金额', 'od_amt':'不良金额'})
    
        dfs = [all_1, all_2, all_3, all_4, all_5, all_6]
        result = pd.concat(dfs, axis=1).fillna(0)
        result.index.name=all_1.index.name # pandas的bug https://github.com/pandas-dev/pandas/issues/13475
        
        return(result)

    # 每月数据
    data_all = od_template(db_data, gp_keys_all)
    
    # 当月金额范围数据
    db_data_last = db_data[db_data.data_dt == db_data.data_dt.max()]
    
    last_1 = db_data_last[(db_data_last.overdue_status_3 != 2)][gp_keys_last+['cnt', 'loan_pr', 'sp_amt', 'bal']].groupby(gp_keys_last).sum().rename(
            columns={'cnt':'未结清户数', 'loan_pr':'放款总额', 'sp_amt':'应还总额', 'bal':'贷款余额'})
    last_2 = db_data_last[(db_data_last.overdue_status_3 == 1)][gp_keys_last+['cnt', 'od_amt']].groupby(gp_keys_last).sum().rename(
            columns={'cnt':'逾期户数', 'od_amt':'逾期金额'})
    
    dfs_last = [last_1, last_2]
    data_last = pd.concat(dfs_last, axis=1).fillna(0)

    # 当月各省数据
    if gp_keys_prov:
        data_prov = od_template(db_data_last, gp_keys_prov)
    
    # 返回结果
    return([_translate(data_all,dct_dimension,dct_col), 
            _translate(data_last,dct_dimension,dct_col),
            _translate(data_prov,dct_dimension,dct_col) if gp_keys_prov else None])

def overdue_toukong(db_data, dct_dimension, dct_col, gp_keys_all):
    """投控逾期不良"""
    
    all_1 = db_data[gp_keys_all+['bal_prin']].groupby(gp_keys_all).sum().rename(
            columns={'bal_prin':'期末本金'})
    all_2 = db_data[(db_data.new_maturity_days > 0)][gp_keys_all+['od_principal_0']].groupby(gp_keys_all).sum().rename(
            columns={'od_principal_0':'逾期本金'})
    all_3 = db_data[(db_data.new_maturity_days > 2)][gp_keys_all+['od_principal_0']].groupby(gp_keys_all).sum().rename(
            columns={'od_principal_0':'不良本金'})
    
    dfs_all = [all_1, all_2, all_3]
    data_all = pd.concat(dfs_all, axis=1).fillna(0)
    data_all['逾期率'] = data_all['逾期本金'] / data_all['期末本金']
    data_all['不良率'] = data_all['不良本金'] / data_all['期末本金']
    
    # 返回结果
    return(_translate(data_all,dct_dimension,dct_col))
    
def status_trans(db_data, dct_dimension, dct_col, index_values, pivot_values_all, pivot_values_trans):
    """状态迁徙表"""
    
    # 翻译维度
    db_data = _translate(db_data, dct_dimension, dct_col, True)
    
    # 总体状态==========
    all_1 = db_data.pivot_table(values=pivot_values_all, index=index_values, columns=['new_loan'], aggfunc='sum')
    all_2 = db_data.pivot_table(values=pivot_values_all, index=index_values, columns=['overdue_status_5'], aggfunc='sum')
    all_3 = db_data.pivot_table(values=pivot_values_all, index=index_values, columns=['overdue_status_3'], aggfunc='sum')

    dfs_all = [all_1, all_2, all_3]
    data_all = pd.concat(dfs_all, axis=1)[pivot_values_all].fillna(0)
    
    # 特殊处理
    data_all.columns.set_names([None,None], inplace =True)
    data_all.rename(columns=dct_col, level=0, inplace=True)
    
    # 状态迁徙==========
    db_data['trans_nl_od'] = db_data.new_loan + '-' + db_data.overdue_status_3
    db_data['trans_od_3'] = db_data.overdue_status_3_last + '-' + db_data.overdue_status_3
    db_data['trans_od_5'] = db_data.overdue_status_5_last + '-' + db_data.overdue_status_5
    db_data['trans_status'] = db_data.status_last_month + '-' + db_data.status_this_month
    
    # 过滤nan
    db_data_trans = db_data[~(db_data['trans_nl_od'].str.contains('nan') |
                              db_data['trans_od_3'].str.contains('nan') |
                              db_data['trans_od_5'].str.contains('nan') |
                              db_data['trans_status'].str.contains('nan'))]
    
    trans_1 = db_data_trans.pivot_table(values=pivot_values_trans, index=index_values, columns=['trans_nl_od'], aggfunc='sum').loc[
            :,(slice(None),['新增贷款-逾期'])]
    trans_2 = db_data_trans.pivot_table(values=pivot_values_trans, index=index_values, columns=['trans_od_3'], aggfunc='sum').loc[
            :,(slice(None),['逾期-结清','逾期-逾期','逾期-非逾期','非逾期-结清','非逾期-逾期','非逾期-非逾期'])]
    trans_3 = db_data_trans.pivot_table(values=pivot_values_trans, index=index_values, columns=['trans_od_5'], aggfunc='sum').loc[
            :,(slice(None),['一般-一般','一般-催收','一般-严重','催收-一般','催收-催收','催收-严重','严重-严重'])]
    trans_4 = db_data_trans.pivot_table(values=pivot_values_trans, index=index_values, columns=['trans_status'], aggfunc='sum').loc[
            :,(slice(None),['活动状态(active)-终止(terminate)'])]
    
    temp_index = trans_2.index if len(trans_2) > len(trans_4) else trans_4.index
    trans_5 = (trans_2.loc[:,(slice(None), ['逾期-逾期'])].reindex(index=temp_index).rename(columns={'逾期-逾期':'正常出账'}) - 
               trans_4.loc[:,(slice(None), ['活动状态(active)-终止(terminate)'])].reindex(index=temp_index).rename(
                       columns={'活动状态(active)-终止(terminate)':'正常出账'}))
    
    dfs_trans = [trans_1, trans_2, trans_3, trans_4, trans_5]
    data_trans = pd.concat(dfs_trans, axis=1)[pivot_values_trans].fillna(0)

    # 特殊处理
    data_trans.columns.set_names([None,None], inplace =True)
    data_trans.rename(columns=dct_col, level=0, inplace=True)

    return([_translate(data_all,dct_dimension,dct_col), 
            _translate(data_trans,dct_dimension,dct_col)])

def vintage(db_data, dct_dimension, dct_col, gp_keys_all):
    """vintage表"""
    
    if gp_keys_all == ['prov_cd']: # 特殊处理 prov_cd
        # 金额
        all_1 = db_data[gp_keys_all+['begin_date']].groupby(gp_keys_all).min().sort_values('begin_date').rename(columns=dct_col)
        all_2 = db_data.pivot_table(values='od_amt', index=gp_keys_all, columns=['data_dt'], aggfunc='sum')
        
        dfs_all = [all_1, all_2]
        data_all_value = pd.concat(dfs_all, axis=1).reindex(all_1.index)
        data_all_value = _patch(data_all_value.set_index([data_all_value.index, dct_col['begin_date']]), 
                               pd.Index(data_all_value[dct_col['begin_date']]))
        
        # 比例
        all_3 = db_data.pivot_table(values='loan_pr', index=gp_keys_all, columns=['data_dt'], aggfunc='sum')
        temp_pct = all_2 / all_3
        
        dfs_all = [all_1, temp_pct]
        data_all_pct = pd.concat(dfs_all, axis=1).reindex(all_1.index)
        data_all_pct = _patch(data_all_pct.set_index([data_all_pct.index, dct_col['begin_date']]), 
                             pd.Index(data_all_pct[dct_col['begin_date']]))
        
    elif gp_keys_all == ['stage', 'begin_date']: # 特殊处理 stage
        # 获取节点月末日期
        lst_month_break = sorted([pd.datetime.strptime(x.split(',')[0][-10:], '%Y/%m/%d') + pd.tseries.offsets.MonthEnd()
                            for x in dct_dimension['stage'].values()])
        
        # 金额
        data_all_value = _patch(db_data.pivot_table(values='od_amt', index=gp_keys_all[0], columns=['data_dt'], aggfunc='sum'),
                               pd.DatetimeIndex([x.strftime('%Y/%m/%d') for x in lst_month_break])) #TODO：考虑周频率
        
        # 比例
        temp_value = _patch(db_data.pivot_table(values='loan_pr', index=gp_keys_all[0], columns=['data_dt'], aggfunc='sum'),
                           pd.DatetimeIndex([x.strftime('%Y/%m/%d') for x in lst_month_break]))
        data_all_pct = data_all_value / temp_value
        
    else: # 一般情况
        # 金额
        all_1 = db_data[db_data.data_dt == db_data.data_dt.max()][gp_keys_all+['loan_pr']].groupby(gp_keys_all).sum().rename(
                columns={'loan_pr':'新增放款金额'})
        all_2 = _patch(db_data.pivot_table(values='od_amt', index=gp_keys_all, columns=['data_dt'], aggfunc='sum'))
        
        dfs_all = [all_1, all_2]
        data_all_value = pd.concat(dfs_all, axis=1)
        
        # 比例
        data_all_pct = data_all_value.apply(lambda x: x/data_all_value['新增放款金额']).replace([pd.np.inf, -pd.np.inf], pd.np.nan)
        data_all_pct.iloc[:,0] = data_all_value.iloc[:,0]
        
    return([_translate(data_all_value,dct_dimension,dct_col), 
            _translate(data_all_pct,dct_dimension,dct_col)])

def vintage_toukong(db_data, dct_dimension, dct_col, gp_keys_all):
    """vintage表"""
    
    def _fill_upper(df):
        """vintage左上半三角补零"""
        for i in range(df.shape[0]):
            for j in range(df.shape[1]):
                if i+j <= df.shape[0] and pd.isnull(df.iloc[i,j]):
                    df.iloc[i,j] = 0
        return(df)
    
    all_1 = db_data[db_data.data_dt == db_data.data_dt.max()][gp_keys_all+['loan_pr']].groupby(gp_keys_all).sum().rename(
            columns={'loan_pr':'新增放款金额'})
    all_2 = _patch(db_data[db_data.new_maturity_days == 0].pivot_table(values='od_amt_0', index=gp_keys_all, columns=['data_dt'], aggfunc='sum'))
    all_3 = _patch(db_data[db_data.new_maturity_days >= 1].pivot_table(values='od_amt_0', index=gp_keys_all, columns=['data_dt'], aggfunc='sum'))
    all_4 = _patch(db_data[db_data.new_maturity_days >= 3].pivot_table(values='od_amt_0', index=gp_keys_all, columns=['data_dt'], aggfunc='sum'))
    
    data_all_0 = pd.concat([all_1, all_2], axis=1)
    data_all_30 = pd.concat([all_1, all_3], axis=1)
    data_all_90 = pd.concat([all_1, all_4], axis=1)
        
    return([_fill_upper(_translate(data_all_0,dct_dimension,dct_col)), 
            _fill_upper(_translate(data_all_30,dct_dimension,dct_col)),
            _fill_upper(_translate(data_all_90,dct_dimension,dct_col))])

def reloan(db_data, dct_dimension, dct_col, gp_keys_mcd, gp_keys_db):
    """续贷历史情况，只附加在首续贷表中"""

    # 按商户统计
    mcd_1 = db_data[(db_data.reloantimes == 2)][gp_keys_mcd+['cnt']].groupby(gp_keys_mcd).sum().rename(
            columns={'cnt':'续贷户数'})
    mcd_2 = db_data[(db_data.reloantimes == 1) & (db_data.overdue_status_3 == 2)][gp_keys_mcd+['cnt']].groupby(gp_keys_mcd).sum().rename(
            columns={'cnt':'结清户数'})

    dfs_mcd = [mcd_1, mcd_2]
    data_mcd = pd.concat(dfs_mcd, axis=1).fillna(0)
    data_mcd.loc[:,'续贷率'] = data_mcd['续贷户数'] / data_mcd['结清户数']
    
    # 按借据统计
    db_1 = db_data[(db_data.new_loan == 1) & (db_data.reloan > 1)][gp_keys_db+['cnt', 'loan_pr']].groupby(gp_keys_db).sum().rename(
            columns={'cnt':'续贷人次', 'loan_pr':'续贷金额'})
    db_2 = db_data[(db_data.reloantimes != 1)][gp_keys_mcd+['cnt', 'loan_pr']].groupby(gp_keys_mcd).sum().rename(
            columns={'cnt':'累计续贷人次', 'loan_pr':'累计续贷金额'}) # gp_keys_mcd没错

    dfs_db = [db_1, db_2]
    data_db = pd.concat(dfs_db, axis=1).fillna(0)
    
    return([_translate(data_mcd,dct_dimension,dct_col), 
            _translate(data_db,dct_dimension,dct_col)])

def resize_sheets(file_path):
    """Sheets列宽自适应"""
    
    writer = xw.Book(file_path)
    [x.autofit() for x in writer.sheets]
    writer.save()
    writer.app.quit()

#%%
if __name__=='__main__':

    # 连接数据库
    engine_oracle = create_engine(config.ConfigDevelopment.DB_ORACLE['str'], 
                                  connect_args={'encoding':'utf8', 'nencoding':'utf8'})
    
    # 获取字典
    db_dimension = pd.read_sql("select * from risk_dimension", engine_oracle)
    
    dct_dimension = {db_dimension.iloc[x,0].lower():{} for x in range(len(db_dimension))}
    [dct_dimension[db_dimension.iloc[x,0].lower()].update({db_dimension.iloc[x,1]:db_dimension.iloc[x,2]}) for x in range(len(db_dimension))]
    dct_dimension['prov_cd'] = {str(x):y for x,y in dct_dimension['prov_cd'].items()} # 省市在stat_all里是字符，在dimension里是数字
    
    dct_col = {'data_dt':'月份',
               'cnt':'户数',
               'od_amt':'金额',
               'diff_od_amt':'逾期金额增量',
               'loan_pr':'贷款本金',
               'loan_pr_scope':'本金范围',
               'new_amt':'新增放款金额',
               'begin_date':'放款日期',
               'aipmchttype':'产品类型',
               'repay_period':'还款方式',
               'prov_cd':'省市',
               'white':'白户',
               'applysource':'特例违例',
               'reloantimes':'首续贷',
               'stage':'阶段',
               'light':'红黄绿灯',
               'loan_period_mon':'贷款期长'}

    #%% 周末报表==============================
    # 更新最近一个周四的数据，通过参数n调整最近第几个周四
    dt_last_thu = pd.datetime.today() - pd.tseries.offsets.Week(n=1, weekday=3, normalize=True)
    db_data_dt = pd.read_sql("select distinct data_dt from thbl.risk_statistics_all", engine_oracle)
    if dt_last_thu not in set(db_data_dt.data_dt):
        with engine_oracle.begin() as conn:
            conn.execute("call RISK_STAT_MONTH('{0}',0)".format(dt_last_thu.strftime('%Y%m%d')))
            print('新增周四数据：' + dt_last_thu.strftime('%F'))

    # 获取周末数据
    sql = "select * from thbl.risk_statistics_all where data_dt = TO_DATE('"+dt_last_thu.strftime('%Y%m%d')+"', 'YYYYMMDD')"
    db_week_end = pd.read_sql(sql, engine_oracle)
    
    # 周末报表
    db_data = db_week_end.copy()
    overdue_weekly = overdue(db_data, dct_dimension, dct_col, ['data_dt'], ['loan_pr_scope'], ['prov_cd'])
    
    str_file_name = '风险周报_' + dt_last_thu.strftime('%Y%m%d') + '.xlsx'
    with pd.ExcelWriter(str_file_name, datetime_format='yyyy-mm-dd') as writer:
        overdue_weekly[0].to_excel(writer, sheet_name='逾期不良', startrow=0, startcol=0)
        overdue_weekly[1].to_excel(writer, sheet_name='逾期不良', startrow=0, startcol=overdue_weekly[0].shape[1]+5)
        overdue_weekly[2].to_excel(writer, sheet_name='逾期不良', startrow=5, startcol=0)

    resize_sheets(str_file_name)

    #%% 月末报表==============================
    # 更新月末数据：可修改refresh_all全部刷新
    refresh_all = False
    lst_month = pd.date_range('20151101',pd.datetime.today(),freq='m')
    db_data_dt = pd.read_sql("select distinct data_dt from thbl.risk_statistics_all", engine_oracle)
    target_month = lst_month if refresh_all else set(lst_month)-set(db_data_dt.data_dt)
    with engine_oracle.begin() as conn:
        for dt in target_month:
            conn.execute("call RISK_STAT_MONTH('{0}',0)".format(dt.strftime('%Y%m%d')))
            print('新增月末数据：' + dt.strftime('%F'))

    # 获取月末数据
    str_month = ', '.join(["TO_DATE('"+x.strftime('%Y%m%d')+"', 'YYYYMMDD')" for x in lst_month])
    sql = "select * from thbl.risk_statistics_all where data_dt in ({0})".format(str_month)
    db_month_end = pd.read_sql(sql, engine_oracle)
    
    # 投控报表
    db_data = db_month_end.copy()
    toukong_overdue = overdue_toukong(db_data, dct_dimension, dct_col, ['data_dt'])
    toukong_vintage = vintage_toukong(db_data, dct_dimension, dct_col, ['begin_date'])
    
    str_file_name = '风险月报_' + pd.datetime.today().strftime('%Y%m%d') + '_投控.xlsx'
    with pd.ExcelWriter(str_file_name, datetime_format='yyyy年mm月') as writer:
        toukong_overdue.to_excel(writer, sheet_name='逾期不良', startrow=0, startcol=0)
        toukong_vintage[0].to_excel(writer, sheet_name='全国30天以下资产情况', startrow=0, startcol=0)
        toukong_vintage[1].to_excel(writer, sheet_name='全国30天以上资产情况', startrow=0, startcol=0)
        toukong_vintage[2].to_excel(writer, sheet_name='全国90天以上资产情况', startrow=0, startcol=0)

    resize_sheets(str_file_name)

    #%% 套表
    for gp in ['', 'aipmchttype', 'loan_period_mon', 'repay_period', 'white', 'applysource', 'reloantimes', 'light', 'prov_cd', 'stage']:
        
#        # 调试
#        if gp not in ['aipmchttype', 'stage']:
#            continue 
        
        db_data = db_month_end.copy()
        
        # 特殊处理
        if gp=='aipmchttype':
            db_data[gp] = db_month_end[gp].map(lambda x: 1 if x==3 else x)
        
        if gp=='reloantimes':
            data_reloan = reloan(db_data, dct_dimension, dct_col, ['data_dt'], ['begin_date'])
            db_data[gp] = db_month_end[gp].map(lambda x: '首贷' if x==1 else '续贷')
            
        #     # 临时增加：首续贷_特例违例
        #     data_overdue_temp = overdue(db_data, dct_dimension, dct_col, ['reloantimes', 'applysource', 'data_dt'], ['reloantimes', 'applysource', 'loan_pr_scope'])
        #     with pd.ExcelWriter('风险报表_首续贷_特例违例.xlsx', datetime_format='yyyy年mm月') as writer:
        #         data_overdue_temp[0].to_excel(writer, sheet_name='逾期不良')

        # 资产情况
        if gp=='prov_cd':
            # vintage表取全量数据
            data_vintage_all = vintage(db_data, dct_dimension, dct_col, [gp])
            data_vintage_ex_xiamen = vintage(db_data[db_data.prov_cd!='3502'], dct_dimension, dct_col, [gp])
            
            # 剩余表只取当月数据
            db_data = db_month_end[db_month_end.data_dt == db_month_end.data_dt.max()]
        else:
            data_vintage_all = vintage(db_data, dct_dimension, dct_col, [gp, 'begin_date']) if gp else \
                               vintage(db_data, dct_dimension, dct_col, ['begin_date'])
                               
            data_vintage_ex_xiamen = vintage(db_data[db_data.prov_cd!='3502'], dct_dimension, dct_col, [gp, 'begin_date']) if gp else \
                                     vintage(db_data[db_data.prov_cd!='3502'], dct_dimension, dct_col, ['begin_date'])
        
        # 逾期不良
        data_overdue = overdue(db_data, dct_dimension, dct_col, [gp, 'data_dt'], [gp, 'loan_pr_scope']) if gp else \
                       overdue(db_data, dct_dimension, dct_col, ['data_dt'], ['loan_pr_scope'])
    
        # 状态迁徙
        data_trans = status_trans(db_data, dct_dimension, dct_col, [gp, 'data_dt'], ['cnt', 'od_amt'], ['cnt', 'diff_od_amt']) if gp else \
                     status_trans(db_data, dct_dimension, dct_col, ['data_dt'], ['cnt', 'od_amt'], ['cnt', 'diff_od_amt'])

        # 输出
        str_file_name = '风险月报_' + pd.datetime.today().strftime('%Y%m%d') + ('_' + dct_col[gp] if gp in dct_col else '') + '.xlsx'
        
        with pd.ExcelWriter(str_file_name, datetime_format='yyyy年mm月') as writer:
            data_overdue[0].to_excel(writer, sheet_name='逾期不良', startrow=0, startcol=0)
            data_overdue[1].to_excel(writer, sheet_name='逾期不良', startrow=0, startcol=data_overdue[0].shape[1]+5)
    
            data_trans[0].to_excel(writer, sheet_name='状态迁徙', startrow=0, startcol=0)
            data_trans[1].to_excel(writer, sheet_name='状态迁徙', startrow=data_trans[0].shape[0]+5, startcol=0)
    
            data_vintage_all[0].to_excel(writer, sheet_name='全国资产情况', startrow=0, startcol=0)
            data_vintage_all[1].to_excel(writer, sheet_name='全国资产情况', startrow=data_vintage_all[0].shape[0]+5, startcol=0)
    
            data_vintage_ex_xiamen[0].to_excel(writer, sheet_name='非厦门资产情况', startrow=0, startcol=0)
            data_vintage_ex_xiamen[1].to_excel(writer, sheet_name='非厦门资产情况', startrow=data_vintage_ex_xiamen[0].shape[0]+5, startcol=0)
            
            if gp=='reloantimes':
                data_reloan[0].to_excel(writer, sheet_name='续贷历史情况', startrow=0, startcol=0)
                data_reloan[1].to_excel(writer, sheet_name='续贷历史情况', startrow=data_reloan[0].shape[0]+5, startcol=0)
    
        resize_sheets(str_file_name)
