# %% [Markdown]
# A2 DEBUG TASK: 
##  Trace an empty chart Through a Simple Pipeline and trace back source data.

# %% [Markdown]
'''
This is a chart in our daily report, the position count(C) is all 0 (in the graph, ‘C_Idx’, ‘C_<1%’ etc. are not plotted on the graph), find the reason behind this.  

If there is a bug in A2.py, explain where and change and comment on the script if needed. Do not change the input and output of ‘prepare_position_data’ and ‘pm_hist’ function. 

'''

# %% [Markdown]
'''
Function below contains the functions to process data from citcoposition_OA.csv. The output is  pm_hist_OA.csv and is used for future analysis testing.
The chart uses the data from column named “num_pos” in pm_hist_OA.csv . Only a subset of the time period is included, rather than the full time series.  
'''

# %%
# ==================== load packages ==================== 
import sys
import pandas as pd
from pandas.tseries.offsets import BDay
import numpy as np
import os
from datetime import datetime, timedelta
import logging
import time

import os
from pathlib import Path

# %%
def prepare_position_data(df):

    # set residual threshold to remove residual positions
    threshold_residual = 1e-4

    # set exclusion list of exotic options

    df_citcoposition = df
        
    # ==================== data processing (simplified) ====================

    # Split options and non-options 
    df_options = df_citcoposition[(df_citcoposition['TradeType'].str.contains('|'.join(['Option', 'Options'])))].copy()
    df_options['OptionsFlag'] = 1
    df_non_options = df_citcoposition[~df_citcoposition['TradeType'].str.contains('|'.join(['Option', 'Options']))].copy()
    df_non_options['OptionsFlag'] = 0

    # Fallback: NMV for ALL positions (including options) = EndMarketValue
    df_options['NMV'] = df_options['EndMarketValue']
    df_non_options['NMV'] = df_non_options['EndMarketValue']

    # Recombine
    df_all_pos = pd.concat([df_non_options, df_options], ignore_index=True)

    # Underlying selection 
    df_all_pos['Underlying'] = np.where(
        df_all_pos['TradeType'] == 'Equity', df_all_pos['UnderlyingId'],
        np.where(df_all_pos['UnderlyingCode'] != '', df_all_pos['UnderlyingCode'], df_all_pos['UnderlyingId'])
    )

    # Proper() last token
    df_all_pos['Underlying'] = (df_all_pos['Underlying'].str.rsplit(' ', n=1).str[0] + ' ' +
                                df_all_pos['Underlying'].str.split().str[-1].str.capitalize())

    # Simplified logic, no need to dig deep into this
    df_all_pos['ChildTicker'] = df_all_pos['Underlying']
    df_all_pos['ParentTicker'] = df_all_pos['ChildTicker']  
    df_all_pos['ETFIndex'] = np.where(
        df_all_pos['Underlying'].str.contains(r'\bindex\b', case=False, na=False),
        1, 0
    )
    df_all_pos['AUM'] = 1000000000

    # Fill if any missing due to unexpected types:
    df_all_pos['OptionsFlag'] = df_all_pos['OptionsFlag'].fillna(0)

    # ============ Aggregate to df_agg_pos ============
    df_agg_pos = df_all_pos.groupby(
        ['PositionDate', 'Trader', 'TradeType', 'ParentTicker', 'ChildTicker',
        'ETFIndex', 'OptionsFlag']
    ).agg(
        StartQty=('StartQuantity', 'sum'),
        EndQty=('EndQuantity', 'sum'),
        StartMV=('StartMarketValue', 'sum'),
        NMV=('NMV', 'sum'),
        PnL=('PnL', 'sum'),
        PnL_TB=('PnLTradingBase', 'sum'),
        AUM=('AUM', 'first')
    ).reset_index()

    df_agg_pos = df_agg_pos[['PositionDate', 'Trader', 'TradeType',
                            'ParentTicker', 'ChildTicker', 
                            'ETFIndex', 'OptionsFlag', 'StartQty', 'EndQty', 'StartMV',
                            'NMV', 'PnL', 'PnL_TB', 'AUM']]
    df_agg_pos.rename(columns={'PositionDate': 'ReportDate',
                            'Trader': 'PM',
                            'TradeType': 'InstrumentType'}, inplace=True)



    # remove closed positions with residual MV or PnL
    # abs(StartMV) < 1e-4 and abs(NMV) < 1e-4 and StartQty = 0 and EndQty = 0 and abs(PnL_TB) < 1e-4
    df_agg_pos = df_agg_pos[~((abs(df_agg_pos['StartMV']) < threshold_residual) & (abs(df_agg_pos['NMV']) < threshold_residual) & \
                            (df_agg_pos['StartQty'] == 0) & (df_agg_pos['EndQty'] == 0) & \
                            (abs(df_agg_pos['PnL_TB']) < threshold_residual) & \
                            (abs(df_agg_pos['PnL']) < threshold_residual))]

    return df_agg_pos


#%%
def pm_hist(df):    
    df_agg_pos=df.copy()

    special_pms = {'SID', 'HKP', 'ITT', 'SAC', 'MRD'}
    df_agg_pos = df_agg_pos[~df_agg_pos['PM'].isin(special_pms)]
  
    df_agg_pos = (
        df_agg_pos.groupby(['ReportDate', 'PM', 'ParentTicker', 'ETFIndex'], dropna=False)
        .agg(
            NMV=('NMV', 'sum'),
            PnL=('PnL', 'sum'),
            PnL_TB=('PnL_TB', 'sum'),
            AUM=('AUM', 'mean')   
        )
        .reset_index()
    )

    df_agg_pos['GMV'] = df_agg_pos['NMV'].abs()
    df_agg_pos['GMV_pct'] = df_agg_pos['GMV'] / df_agg_pos['AUM']

    # catetorize ParentTicker, with index if ETFIndex == 1, G<1% if GMV_pct < 1%,
    # G_1%-3% if GMV_pct >= 1% and GMV_pct < 3%, G_3%-5% if GMV_pct >= 3% and GMV_pct < 5%
    # G_>=5%
    df_agg_pos['TickerCat'] = np.where(df_agg_pos['ETFIndex'] == 1, 'Index', \
        np.where(df_agg_pos['GMV_pct'] < 0.01, 'GMV < 1%', \
            np.where(df_agg_pos['GMV_pct'] < 0.03, 'GMV 1%-3%', \
                np.where(df_agg_pos['GMV_pct'] < 0.05, 'GMV 3%-5%', 'GMV >=5%'))))
    
    # count unique tickers
    df_agg_cat = df_agg_pos.groupby(['ReportDate', 'TickerCat', 'PM']).agg(\
        GMV_pct = pd.NamedAgg('GMV_pct', 'sum'),
        num_pos = pd.NamedAgg('ParentTicker', 'nunique')).reset_index()
        
    df_agg_pos_child=df.copy()

    df_agg_pos_child = df_agg_pos_child[df_agg_pos_child['PM'].isin(special_pms)]
  
    df_agg_pos_child = (
        df_agg_pos_child.groupby(['ReportDate', 'PM', 'ChildTicker', 'ETFIndex'], dropna=False)
        .agg(
            NMV=('NMV', 'sum'),
            PnL=('PnL', 'sum'),
            PnL_TB=('PnL_TB', 'sum'),
            AUM=('AUM', 'mean')   
        )
        .reset_index()
    )
    
    df_agg_pos_child['GMV'] = df_agg_pos_child['NMV'].abs()
    df_agg_pos_child['GMV_pct'] = df_agg_pos_child['GMV'] / df_agg_pos_child['AUM']

    df_agg_pos = pd.concat([df_agg_pos, df_agg_pos_child])

    df_agg_cat = df_agg_pos.groupby(['ReportDate', 'TickerCat', 'PM']).agg(\
        GMV_pct = pd.NamedAgg('GMV_pct', 'sum'),
        num_pos = pd.NamedAgg('ParentTicker', 'nunique')).reset_index()
    
    df_agg_cat.sort_values(by = ['ReportDate', 'PM', 'TickerCat'], inplace = True)

    # pivot the df_agg_cat and make TickerCat as 2 sets of columns, with GMV_pct and num_pos as values
    # name the new columns accoring to the TickerCat
    # add (GMV) and (num_pos) as suffix to the column names
    df_agg_cat_pivot = df_agg_cat.pivot(index = ['ReportDate', 'PM'], columns = 'TickerCat', values = ['GMV_pct', 'num_pos']).reset_index()
    df_agg_cat_pivot.columns = ['_'.join(col).strip() for col in df_agg_cat_pivot.columns.values]
    
    # fill na with 0
    df_agg_cat_pivot = df_agg_cat_pivot.fillna(0)

    # rename the columns
    df_agg_cat_pivot.rename(columns = {'ReportDate_': 'ReportDate', 'PM_': 'PM'}, inplace = True)

    # Ensure all expected GMV category columns exist (fix for missing categories in pivot table)
    expected_categories = ['GMV < 1%', 'GMV 1%-3%', 'GMV 3%-5%', 'GMV >=5%', 'Index']
    for category in expected_categories:
        for metric in ['GMV_pct', 'num_pos']:
            col_name = f"{metric}_{category}"
            if col_name not in df_agg_cat_pivot.columns:
                df_agg_cat_pivot[col_name] = 0

    # get the PnL data
    df_pnl = df_agg_pos.groupby(['ReportDate', 'PM']).agg(\
        NMV = pd.NamedAgg('NMV', 'sum'),
        GMV = pd.NamedAgg('GMV', 'sum'),
        PnL = pd.NamedAgg('PnL', 'sum'),
        PnL_TB = pd.NamedAgg('PnL_TB', 'sum'),
        AUM = pd.NamedAgg('AUM', 'first')).reset_index()

    # get NMV/GMV percentage
    df_pnl['NMV_GMV'] = df_pnl['NMV'] / df_pnl['GMV']

    # merge df_pnl with df_agg_cat_pivot    
    df_pnl = df_pnl.merge(df_agg_cat_pivot, on = ['ReportDate', 'PM'], how = 'left')

    df_pnl = df_pnl.fillna(0)


    return df_pnl

#%%
# Main logic
df_citcoposition = pd.read_csv(r"data2\citcoposition_OA.csv")
df1 = prepare_position_data(df_citcoposition)
df2 = pm_hist(df1)
df2.to_csv(r'data2\pm_hist_OA_wrong.csv', index = False)


#%% [Markdown]
## A2.1 answer
#%%
# A2.1
# change the code if necessary, do not change the input and output of the function
A2_1 = "Explain the reason here"


# %% [Markdown]
## A2.2 answer
# %%
# A2.2
# Do not change the logic of Q2.1, can write othe helper function to help you trace back the data
# we mainly check whether you flagged out the correct data and your clarity
A2_2 = 'write here'
# use pandas to filter out the traced back source data
df_chosen = df_citcoposition

