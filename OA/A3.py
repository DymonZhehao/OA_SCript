import pandas as pd

def trade_stats_update(start_date, end_date, PM_sel, conn, csv_path):
    # conn is the database name
    df_trade_stats = pd.read_csv(csv_path)
    df_trade_stats["trade_end_date"] = pd.to_datetime(df_trade_stats["trade_end_date"], errors="coerce")
    start_ts = pd.to_datetime(start_date)
    df_trade_stats_affected = df_trade_stats[
        (df_trade_stats["PM"] == PM_sel) &
        (df_trade_stats["trade_end_date"] >= start_ts)
    ].copy()

    # get the earliest trade_start_date among these affected trades
    trade_daily_start_date = df_trade_stats_affected['trade_start_date'].min()
    # make ticker_list into a tuple for sql query
    ticker_list = tuple(df_trade_stats_affected['ParentTicker'].unique().tolist())

    df_trade_affected = pd.DataFrame()
    if len(ticker_list):
        # query trade daily data for these affected trades from their trade_start_date to report_date        
        sql_trade_affected = f"SELECT *\
        FROM trade_daily_cor_strategy\
        WHERE PM = '{PM_sel}' AND ParentTicker IN {ticker_list} AND \
            PositionDate >= '{trade_daily_start_date}' AND \
            PositionDate <= '{end_date}'"
        df_trade_affected = pd.read_sql(sql_trade_affected, conn)
        print(f"Number of trade daily records to process for affected trades: {len(df_trade_affected)}")
    else:        
        print("No affected tickers to process.")

    trade_id_update_list = tuple(df_trade_stats[df_trade_stats['_merge'] == 'both']['trade_id_y'].tolist())
    
    if len(trade_id_update_list) > 0:
        sql_delete_live_trades = f"DELETE FROM trade_daily_cor_strategy \
            WHERE PM = '{PM_sel}' AND trade_id in {trade_id_update_list}"
        result = pd.read_sql(sql_delete_live_trades, conn)

    return df_trade_affected, result

'''
This function looks up all trades for a given PM that ended after a specified start date, 
identifies which tickers are affected, and finds the earliest start date among those trades. 
It then pulls the corresponding daily trade records for those tickers between that earliest 
start date and the given end date. Finally, it returns the daily trade data that needs to 
be updated or recalculated.

Find a bug in the code that might occur when reading the SQL query. Without changing the SQL,
Make a change to the code to fix it. If you decide to write a function, do not use any other 
libraries except for pandas.
'''