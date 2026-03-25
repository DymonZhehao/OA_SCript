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
import string

#%%

import re


def mask_security_id(value: str, rng: np.random.Generator) -> str:
    """
    Replace the first contiguous block of digits in SecurityID with a random number,
    preserving the original digit-width and leaving the rest of the string unchanged.

    Examples:
        '00120 KS Equity' -> '01430 KS Equity' (5 digits preserved)
        '120 HK'          -> '483 HK' (3 digits preserved)
        'ABC XYZ'         -> (no change, no digits)
    """
    if not isinstance(value, str):
        return value

    m = re.search(r'(\d+)', value)
    if not m:
        return value  # no digits to replace

    start, end = m.span()
    digits = m.group(1)
    width = len(digits)

    # Generate a random integer with the same width (allow leading zeros)
    # Range: 0 .. (10^width - 1)
    max_val = 10 ** width - 1
    new_num = rng.integers(0, max_val + 1)
    new_digits = f"{new_num:0{width}d}"

    return value[:start] + new_digits + value[end:]


def mask_dataframe(
    df: pd.DataFrame,
    security_col: str = "SecurityId",
    market_col: str = "StartMarketValue",
    seed: int | None = 42
) -> pd.DataFrame:
    """
    Returns a copy of df with:
      - security_col masked (digits replaced with random digits of same width)
      - market_col augmented with uniform noise in [-100000, 100000]
    """
    rng = np.random.default_rng(seed)
    out = df.copy()

    # 1) Mask SecurityId column
    if security_col in out.columns:
        out[security_col] = out[security_col].apply(lambda x: mask_security_id(x, rng))
        out['UnderlyingBbgCode'] =  out['UnderlyingBbgCode'].apply(lambda x: mask_security_id(x, rng))
        out['InstrumentDescription'] = out['InstrumentDescription'].apply(lambda x: mask_security_id(x, rng))

    else:
        raise KeyError(f"Column '{security_col}' not found in df")
    

    

    out[market_col] = pd.to_numeric(out[market_col], errors="coerce")
    n = len(out)
    noise = rng.uniform(-1000000, 1000000, size=n).round(4)  # float noise
    out[market_col] = noise
    market_col2 = 'EndMarketValue'
    out[market_col2] = pd.to_numeric(out[market_col2], errors="coerce")
    noise = rng.uniform(-1000000, 1000000, size=n).round(4)  # float noise
    out[market_col2] = noise

    market_col3 = 'StartQuantity'
    
    out[market_col3] = pd.to_numeric(out[market_col3], errors="coerce")
    n = len(out)
    noise = rng.integers(-1000000, 1000000, size=n)  # float noise
    out[market_col3] = noise

    market_col4 = 'EndQuantity'
    out[market_col4] = pd.to_numeric(out[market_col4], errors="coerce")
    noise = rng.integers(-1000000, 1000000, size=n)  # float noise
    out[market_col4] = noise

    market_col5 = 'PnL'
    
    out[market_col5] = pd.to_numeric(out[market_col5], errors="coerce")
    n = len(out)
    noise = rng.uniform(-1000000, 1000000, size=n).round(4)  # float noise
    out[market_col5] = noise

    market_col6 = 'PnLTradingBase'
    out[market_col6] = pd.to_numeric(out[market_col6], errors="coerce")
    noise = rng.uniform(-1000, 1000, size=n).round(4)  # float noise
    out[market_col6] = out[market_col5] + noise

    return out

def make_pm_mask_map(pm_series: pd.Series, seed: int = 77) -> dict:
    """
    Build a mapping from each unique PM to a random three-capital-letter code.
    Ensures no duplicates in the generated masks.
    """
    rng = np.random.default_rng(seed)
    unique_pms = pd.Series(pm_series.dropna().unique())

    # Generate unique 3-letter codes
    letters = np.array(list(string.ascii_uppercase))
    masks = set()
    pm_to_mask = {}

    for pm in unique_pms:
        # keep trying until we get a unique code
        while True:
            code = ''.join(rng.choice(letters, size=3))
            if code not in masks:
                masks.add(code)
                pm_to_mask[pm] = code
                break
    return pm_to_mask

# %%
def prepare_position_data():

    # set residual threshold to remove residual positions
    threshold_residual = 1e-4

    # set exclusion list of exotic options
    exotic_options = ['WorstC', 'DualDig', 'Corr', 'VARDis', 'VolDis', 'DisSwa', 'Vol', 'Var']

    df_citcoposition = pd.read_csv(r"data\citcoposition_OA3.csv")

    #df_citcoposition3 = mask_dataframe(df_citcoposition)
    
    df_citcoposition3['PositionDate'] = pd.to_datetime(df_citcoposition3['PositionDate'])

    # Filter between two dates (inclusive)
    start = pd.to_datetime('2022-02-21')
    end   = pd.to_datetime('2022-02-25')

    df_citcoposition3 = df_citcoposition3[
        (df_citcoposition3['PositionDate'] >= start) &
        (df_citcoposition3['PositionDate'] <= end)
    ]

    df_citcoposition3.to_csv(r"data\citcoposition_OA.csv", index = False)
    # --- usage ---
     # Build mapping
    pm_map = make_pm_mask_map(df_citcoposition3['Trader'], seed=20260318)

    # Apply to a new column (recommended to keep original)
    df_citcoposition4 = df_citcoposition3.copy()
    df_citcoposition4['Trader'] = df_citcoposition3['Trader'].map(pm_map)
    #df_citcoposition4.to_csv(r"data\citcoposition_OA.csv", index = False)

        
    # ==================== data processing (simplified) ====================

    # Split options and non-options 
    df_options = df_citcoposition[(df_citcoposition['Type'].str.contains('|'.join(['Option', 'Options']))) &
                                (~df_citcoposition['InstrumentDescription'].str.contains('|'.join(exotic_options)))]
    df_options['OptionsFlag'] = 1
    df_non_options = df_citcoposition[~df_citcoposition['Type'].str.contains('|'.join(['Option', 'Options']))]
    df_non_options['OptionsFlag'] = 0

    # Fallback: NMV for ALL positions (including options) = EndMarketValue
    df_options['NMV'] = df_options['EndMarketValue']
    df_non_options['NMV'] = df_non_options['EndMarketValue']

    # Recombine
    df_all_pos = pd.concat([df_non_options, df_options], ignore_index=True)

    # Underlying selection 
    df_all_pos['Underlying'] = np.where(
        df_all_pos['Type'] == 'Equity', df_all_pos['SecurityId'],
        np.where(df_all_pos['UnderlyingBbgCode'] != '', df_all_pos['UnderlyingBbgCode'], df_all_pos['SecurityId'])
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
        ['PositionDate', 'Trader', 'Type', 'ParentTicker', 'ChildTicker',
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

    df_agg_pos['PMGroup'] = 0
    df_agg_pos = df_agg_pos[['PositionDate', 'Trader', 'PMGroup', 'Type',
                            'ParentTicker', 'ChildTicker', 
                            'ETFIndex', 'OptionsFlag', 'StartQty', 'EndQty', 'StartMV',
                            'NMV', 'PnL', 'PnL_TB', 'AUM']]
    df_agg_pos.rename(columns={'PositionDate': 'ReportDate',
                            'Trader': 'PM',
                            'Type': 'InstrumentType'}, inplace=True)



    # remove closed positions with residual MV or PnL
    # abs(StartMV) < 1e-4 and abs(NMV) < 1e-4 and StartQty = 0 and EndQty = 0 and abs(PnL_TB) < 1e-4
    df_agg_pos = df_agg_pos[~((abs(df_agg_pos['StartMV']) < threshold_residual) & (abs(df_agg_pos['NMV']) < threshold_residual) & \
                            (df_agg_pos['StartQty'] == 0) & (df_agg_pos['EndQty'] == 0) & \
                            (abs(df_agg_pos['PnL_TB']) < threshold_residual) & \
                            (abs(df_agg_pos['PnL']) < threshold_residual))]

    return df_agg_pos



def pm_hist(df):    
    df_agg_pos=df.copy()

    special_pms = {'SID', 'HKP', 'ITT', 'SVC', 'MRD'}
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

    df_agg_pos_child['TickerCat'] = np.where(df_agg_pos_child['ETFIndex'] == 1, 'Index', \
        np.where(df_agg_pos_child['GMV_pct'] < 0.01, 'GMV < 1%', \
            np.where(df_agg_pos_child['GMV_pct'] < 0.03, 'GMV 1%-3%', \
                np.where(df_agg_pos_child['GMV_pct'] < 0.05, 'GMV 3%-5%', 'GMV >=5%'))))
    
    # count unique tickers
    df_agg_cat_child = df_agg_pos_child.groupby(['ReportDate', 'TickerCat', 'PM']).agg(\
        GMV_pct = pd.NamedAgg('GMV_pct', 'sum'),
        num_pos = pd.NamedAgg('ChildTicker', 'nunique')).reset_index()

    #recombine the data for parent ticker and child ticker
    df_agg_cat = pd.concat([df_agg_cat, df_agg_cat_child])

    df_agg_pos = pd.concat([df_agg_pos, df_agg_pos_child])

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
def main():
    df1 = prepare_position_data()
    df2 = pm_hist(df1)
    df2.to_csv(r'pm_hist_with_error2' + '.csv', index = False)
    print(df2)

    print("CWD:", os.getcwd()) 
    
if __name__ == "__main__":
    main()