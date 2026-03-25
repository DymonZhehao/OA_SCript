#%%
import pandas as pd
import numpy as np

'''
Dataset 1: holdings_raw (df_original)
This table contains the holdings and their current model prices.
Columns:

bbgTicker
price (original model price)

Example:
bbgTicker,price
AAPL US,150
MSFT US,300
GOOGL US,120


Dataset 2: ticker_map.csv (df_override)
This file contains updated override prices, but not all tickers appear here.
Columns:

bbgTicker
price_override

Example:
bbgTicker,price_override
MSFT US,310
GOOGL US,130
TSLA US,200


🎯 Your Task
Write a function:
override_prices(holdings_raw, ticker_map)

The function must:
1️⃣ Merge the two datasets using:
Pythonmerge(..., indicator=True)Show more lines
2️⃣ Override price ONLY when:

the ticker exists in BOTH datasets
otherwise keep the original price

3️⃣ Only keep rows from holdings_raw
(ignore extra tickers in ticker_map.csv)
4️⃣ Return a dataframe with exactly:

bbgTicker
price


📌 Expected Output Example
Using the samples above:





















bbgTickerFinal priceAAPL US150MSFT US310 (override)GOOGL US130 (override)
TSLA is ignored because it does not exist in holdings_raw.
'''


def override_prices(holdings_raw, ticker_map):
    """
    Requirements:
    - MUST use merge(..., indicator=True)
    - Override price only when tickers match
    - Keep only tickers present in holdings_raw
    - Return df with: ['bbgTicker', 'price']
    """

    # Write your solution below:

    return df_final


def override_prices(holdings_raw, ticker_map):

    df = holdings_raw.merge(
        ticker_map.rename(columns={"price_override": "override"}),
        on="bbgTicker",
        how="left",
        indicator=True
    )

    df["price"] = np.where(
        df["_merge"] == "both",   # only override when both have matching ticker
        df["override"],
        df["price"]
    )

    df_final = df[["bbgTicker", "price"]].copy()

    return df_final


# -----------------------------
# Helper: mock Bloomberg lookup
# -----------------------------
def mock_bbg_fetch(underlying: str, bbg_field: str):
    """
    Simulate BBG lookup.
    Replace this with real BBG logic later.
    """
    mock_data = {
        ("AAPL US Equity", "PX_LAST"): 210.5,
        ("MSFT US Equity", "PX_LAST"): 415.2,
        ("700 HK Equity", "PX_LAST"): 325.8,
        ("D05 SG Equity", "PX_LAST"): 28.4,
    }
    return mock_data.get((underlying, bbg_field), np.nan)


# A1.1
# main function
def clean_positions_data(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()

    df = standardize_columns(df)
    df = convert_types(df)

    df,duplicate_flags = find_duplicate_keys(df, key_cols=["date", "bbgticker"])
    df,na_flags = find_na_values(df)

    df =df.reset_index(drop=True)
    issue_flags_df = pd.concat(
        [duplicate_flags,na_flags],
        ignore_index=True
    )

    return df, issue_flags_df


# helper function
def standardize_columns(df: pd.DataFrame) -> pd.DataFrame:
    pass

def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    pass


def find_duplicate_keys(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    pass


def find_na_values(df: pd.DataFrame) -> pd.DataFrame:
    pass





# A1.2
# main function
def merge_positions_with_map(
    positions_df: pd.DataFrame,
    map_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    pass





# A1.3
# main function
def validate_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        df and check_flags_df 
    """
    issue_flags_df = pd.DataFrame()
    df,invalid_df = non_invalid_check(df)
    issue_flags_df = pd.concat([issue_flags_df,invalid_df],ignore_index=True)

    rules = {'price':(None,None), 'numofshares':(None,None)} # replace the None with min and max value you want
    for col in ['price','numofshares']:
       
        wrong_df = range_check(df,col=col,min=rules[col][0], max=rules[col][1]) # check the wrong numbers
        issue_flags_df = pd.concat([issue_flags_df,wrong_df],ignore_index=True)
        
        downbound = None # replace the None with range you want
        upbound = None # replace the None with range you want
        outlier_df = range_check(df,col=col,min=downbound , max=upbound) # check the outlier numbers
        issue_flags_df = pd.concat([issue_flags_df,outlier_df],ignore_index=True)
        issue_flags_df = issue_flags_df.drop_duplicates(keep='first')
    
    return df,issue_flags_df


# helper function
def non_invalid_check(
        df:pd.DataFrame,
)-> tuple[pd.DataFrame, pd.DataFrame]:
    
    pass


def range_check(
        df:pd.DataFrame,
        col:str,
        min:float,
        max:float,
)->  pd.DataFrame:
    pass
    


# A1.4
def update_nan_with_bbg(
    df: pd.DataFrame,
    underlying_col: str,
    target_col: str,
    bbg_field: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        updated_df, summary_df
    """
    df = df.copy()

    if underlying_col not in df.columns or target_col not in df.columns:
        raise KeyError(f"Missing required columns: {underlying_col} or {target_col}")

    updated_count = 0
    failed_count = 0
    summary_rows = []

    nan_mask = df[target_col].isna()

    for idx in df.index[nan_mask]:
        underlying = df.at[idx, underlying_col]
        fetched_value = mock_bbg_fetch(underlying, bbg_field)

        if pd.notna(fetched_value):
            df.at[idx, target_col] = fetched_value
            updated_count += 1
            summary_rows.append({
                "row_index": idx,
                "underlying": underlying,
                "bbg_field": bbg_field,
                "fetched_value": fetched_value,
                "status": "updated"
            })
        else:
            failed_count += 1
            summary_rows.append({
                "row_index": idx,
                "underlying": underlying,
                "bbg_field": bbg_field,
                "fetched_value": np.nan,
                "status": "not_found"
            })

    summary_df = pd.DataFrame(summary_rows)
    print(f"Updated {updated_count} rows, failed to update {failed_count} rows.")

    return df, summary_df


def main():
    raw_positions_df = pd.read_excel("data/Holdings_raw.xlsx")
    raw_map_df = pd.read_excel("data/ticker_map.xlsx")

    # A1.1 Data cleaning
    clean_positions_df, issue_flags_df = clean_positions_data(raw_positions_df)

    # A1.2 Merge and Indicator usage
    merged_df, matched_positions_df = merge_positions_with_map(
        clean_positions_df, raw_map_df
    )

    # A1.3 Validation
    merged_df,check_flags_df = validate_columns(merged_df)

    # A1.4 Update NaN values using BBG
    updated_df, summary = update_nan_with_bbg(
        clean_positions_df,   
        underlying_col="underlying",
        target_col="value",
        bbg_field="PX_LAST"
    )

    print("\n=== Cleaning Issues ===")
    print(issue_flags_df.head())

    print("\n=== Unmatched Positions ===")
    print(matched_positions_df.head())

    print("\n=== Validation Flags ===")
    print(check_flags_df.head())

    print("\n=== BBG Update Summary ===")
    print(summary.head())


if __name__ == "__main__":
    main()
# %%
