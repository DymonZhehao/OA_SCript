#%%
import pandas as pd
import numpy as np


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

#%%
# A1.1
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
    df = df.copy()
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )
    return df


def convert_types(df: pd.DataFrame) -> pd.DataFrame:
    import re
    df = df.copy()

    # convert dates if present
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # numeric candidate columns
    numeric_candidates = ["price", "numberofshares"]
    for col in numeric_candidates:
        if col in df.columns:
            pat = re.compile(r'([+-]?\d+(?:\.\d+)?)')
            s = df[col].astype(str).str.extract(pat, expand=False)
            df[col] = pd.to_numeric(s, errors="coerce")

    # strip string columns
    for col in ['bbgticker','constituentname','industry','supersector']:
        s = df[col]
        df[col] = np.where(s.isna(),s,s.astype(str).str.strip())
    return df


def find_duplicate_keys(df: pd.DataFrame, key_cols: list[str]) -> pd.DataFrame:
    existing_key_cols = [col for col in key_cols if col in df.columns]
    if len(existing_key_cols) == 0:
        return df,pd.DataFrame(columns=df.columns)

    dup_mask = df.duplicated(subset=existing_key_cols, keep='first')
    dup_df = df.loc[dup_mask].copy()

    if dup_df.empty:
        return df,pd.DataFrame(columns=df.columns)


    return df.drop_duplicates(keep='first'),dup_df


def find_na_values(df: pd.DataFrame) -> pd.DataFrame:
    na_mask = df.isna().any(axis=1)
    na_df = df.loc[na_mask].copy()

    if na_df.empty:
        return df,pd.DataFrame(columns=df.columns)

    return df.dropna(),na_df

#%%
# A1.2
def merge_positions_with_map(
    positions_df: pd.DataFrame,
    map_df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    positions_df = positions_df.copy()
    map_df = map_df.copy()
    map_df = standardize_columns(map_df)
    merged_df = positions_df.merge(
        map_df,
        left_on='bbgticker',
        right_on= 'childticker',
        how="left",
        indicator=True
    )

    matched_positions_df = merged_df.loc[merged_df["_merge"] == "both"].copy()

    merged_df['bbgticker'] = np.where(merged_df["_merge"] == "both",merged_df['parentticker'],merged_df['bbgticker'])
    merged_df = merged_df.drop(columns = ['childticker','parentticker','_merge'])

    return merged_df, matched_positions_df

#%%
# A1.3

def validate_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
        flags_df with columns:
        row_index, column_name, value, issue_type, rule_failed
    """
    issue_flags_df = pd.DataFrame()
    df,invalid_df = non_invalid_check(df)
    # issue_flags_df = pd.concat([issue_flags_df,invalid_df],ignore_index=True)

    rules = {'price':(0.0,float('inf'),True,True), 'numofshares':(-float('inf'),float('inf'),True,True)}
    for col in ['price','numofshares']:
       
        wrong_df = range_check(df,col=col,min=rules[col][0], max=rules[col][1],left=rules[col][2],right=rules[col][3]) #check the wrong numbers
        issue_flags_df = pd.concat([issue_flags_df,wrong_df],ignore_index=True)
        
        outlier_df = non_desc(df,col=col) #check the outlier numbers
        issue_flags_df = pd.concat([issue_flags_df,outlier_df],ignore_index=True)
    issue_flags_df = issue_flags_df.drop_duplicates(keep='first')
    
    return df,issue_flags_df


# helper function
def non_invalid_check(
        df:pd.DataFrame,
)-> tuple[pd.DataFrame, pd.DataFrame]:
    
    pattern = r'\*(nan|none|n/?a|null)\*'
   
    mask_all = df.apply(
    lambda s: s.astype("string").str.strip().str.contains(pattern, case=False, na=False))

    invalid_df = df[mask_all.any(axis=1)]
    df = df[~mask_all.any(axis=1)]
    return df, invalid_df


def range_check(
        df:pd.DataFrame,
        col:str,
        min:float,
        max:float,
        left: bool,
        right:bool
)->  pd.DataFrame:
    above_min = (df[col] <= min) if left else (df[col] < min)
    under_max = (df[col] >= max) if right else (df [col] > max)

    mask = above_min | under_max
    issue_df = df[mask]
    return issue_df
    
def non_desc(
        df:pd.DataFrame,
        col:str,
)->  pd.DataFrame: 
      one_day_before_df = df[col].shift(1)
      one_day_before_df = one_day_before_df.fillna(df.loc[0,col])
      mask = df[col] >= one_day_before_df
      return df[~mask]


#%%
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

#%%
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
    validare_df,check_flags_df = validate_columns(merged_df)

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
