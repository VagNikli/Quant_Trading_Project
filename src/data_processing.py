import os
import pandas as pd
import numpy as np

def examine_missing_values(df):
    """
    Examine missing values in every column.
    """
    missing_summary = df.isnull().sum()
    print("\n Missing Values per Column:")
    print(missing_summary[missing_summary > 0])
    return missing_summary

def convert_timestamp_format(df, timestamp_col="Timestamp"):
    """
    Converts timestamp column to a proper datetime format.
    """
    if timestamp_col in df.columns:
        df[timestamp_col] = pd.to_datetime(df[timestamp_col])
        print(f" Converted '{timestamp_col}' to datetime format.")
    else:
        print(f"Warning: Column '{timestamp_col}' not found.")
    return df

def compute_log_returns(df):
    """
    Computes log returns for the 'Close' price.
    """
    df["Log_Returns"] = np.log(df["Close"] / df["Close"].shift(1))
    return df

def process_crypto_data(symbol, raw_data_path="./data/crypto_data", processed_data_path="./data/processed_data"):
    """
    Process raw crypto data:
    - Checks for missing values
    - Converts timestamp format
    - Computes log returns (for mean reversion)
    - Saves the cleaned dataset to 'processed_data/'
    """

    # Define file paths
    raw_file_path = os.path.join(raw_data_path, f"{symbol}_binance_data.csv")
    processed_file_path = os.path.join(processed_data_path, f"{symbol}_processed.csv")

    # Check if file exists
    if not os.path.exists(raw_file_path):
        print(f" Error: File '{raw_file_path}' not found.")
        return None

    # Load data
    df = pd.read_csv(raw_file_path)
    print(f" Loaded raw data for {symbol}: {df.shape[0]} rows.")

    # Check for missing values
    examine_missing_values(df)

    # Convert timestamp
    df = convert_timestamp_format(df)

    # Compute log returns
    df = compute_log_returns(df)

    # Drop NaNs created by log return calculation
    df.dropna(inplace=True)

    # Ensure processed data folder exists
    os.makedirs(processed_data_path, exist_ok=True)

    # Save processed dataset
    df.to_csv(processed_file_path, index=False)
    print(f" Processed data saved to {processed_file_path}")

    return df

# Example Usage:
process_crypto_data("BTCUSDT")
