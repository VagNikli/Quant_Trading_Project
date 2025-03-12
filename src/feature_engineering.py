# Compute technical indicators
import os
import pandas as pd
import numpy as np
from scipy.stats import zscore

def compute_log_returns(df):
    """
    Computes log returns for the 'Close' price.
    """
    df["Log_Returns"] = np.log(df["Close"] / df["Close"].shift(1))
    return df

def compute_z_score(df, column="Close", window=20):
    """
    Computes Z-score (standardized deviation) of a given column.
    """
    df[f"{column}_Mean"] = df[column].rolling(window=window).mean()
    df[f"{column}_Std"] = df[column].rolling(window=window).std()
    df[f"Z_Score_{column}"] = (df[column] - df[f"{column}_Mean"]) / df[f"{column}_Std"]
    return df

def compute_volatility(df, column="Close", window=20):
    """
    Computes rolling standard deviation as a measure of volatility.
    """
    df[f"Volatility_{column}_{window}"] = df[column].rolling(window=window).std()
    return df

def compute_multiple_moving_averages(df, column="Close", windows=[20, 50, 100]):
    """
    Computes simple moving averages (SMA) for multiple specified windows.
    """
    for window in windows:
        df[f"SMA_{window}"] = df[column].rolling(window=window).mean()
    return df

def compute_multiple_exponential_moving_averages(df, column="Close", windows=[20, 50]):
    """
    Computes exponential moving averages (EMA) for multiple specified windows.
    """
    for window in windows:
        df[f"EMA_{window}"] = df[column].ewm(span=window, adjust=False).mean()
    return df

def compute_bollinger_bands(df, column="Close", window=20, num_std=2):
    """
    Computes Bollinger Bands.
    """
    df[f"Bollinger_MA_{window}"] = df[column].rolling(window=window).mean()
    df[f"Bollinger_Upper_{window}"] = df[f"Bollinger_MA_{window}"] + (num_std * df[column].rolling(window=window).std())
    df[f"Bollinger_Lower_{window}"] = df[f"Bollinger_MA_{window}"] - (num_std * df[column].rolling(window=window).std())
    df[f"Bollinger_Width_{window}"] = df[f"Bollinger_Upper_{window}"] - df[f"Bollinger_Lower_{window}"]
    return df

def compute_rsi(df, column="Close", window=14):
    """
    Computes Relative Strength Index (RSI).
    """
    delta = df[column].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    rs = gain / loss
    df[f"RSI_{window}"] = 100 - (100 / (1 + rs))

    # Standardize RSI using Z-score
    df[f"Z_RSI_{window}"] = zscore(df[f"RSI_{window}"].dropna())
    return df

def compute_atr(df, window=14):
    """
    Computes Average True Range (ATR) for volatility measurement.
    """
    df["High-Low"] = df["High"] - df["Low"]
    df["High-Close"] = np.abs(df["High"] - df["Close"].shift(1))
    df["Low-Close"] = np.abs(df["Low"] - df["Close"].shift(1))
    
    df["TrueRange"] = df[["High-Low", "High-Close", "Low-Close"]].max(axis=1)
    df[f"ATR_{window}"] = df["TrueRange"].rolling(window=window).mean()

    return df.drop(columns=["High-Low", "High-Close", "Low-Close", "TrueRange"])

def compute_order_flow_imbalance(df):
    """
    Computes Order Flow Imbalance (OFI).
    """
    df["OFI"] = (df["TakerBuyBaseVolume"] - (df["Volume"] - df["TakerBuyBaseVolume"])) / df["Volume"]
    return df

def compute_volume_delta(df):
    """
    Computes the difference between Taker Buy Volume and Sell Volume.
    """
    df["Volume_Delta"] = df["TakerBuyBaseVolume"] - (df["Volume"] - df["TakerBuyBaseVolume"])
    return df

def process_features(symbol, 
                     processed_data_path="./data/processed_data", 
                     feature_data_path="./data/processed_data",
                     sma_windows=[20, 50, 100],
                     ema_windows=[20, 50],
                     bollinger_windows=[20],
                     rsi_windows=[14],
                     atr_windows=[14]):
    """
    Loads cleaned crypto data and computes technical indicators:
    - Log Returns, Z-score, Volatility
    - Multiple SMAs, EMAs
    - Bollinger Bands, RSI, ATR
    - Saves data with features in 'processed_data/'
    """

    # Define file paths
    processed_file_path = os.path.join(processed_data_path, f"{symbol}_processed.csv")
    feature_file_path = os.path.join(feature_data_path, f"{symbol}_features.csv")

    # Check if file exists
    if not os.path.exists(processed_file_path):
        print(f" Error: Processed file '{processed_file_path}' not found.")
        return None

    # Load processed data
    df = pd.read_csv(processed_file_path)
    print(f" Loaded processed data for {symbol}: {df.shape[0]} rows.")

    # Compute all technical indicators
    df = compute_log_returns(df)
    df = compute_z_score(df)
    df = compute_volatility(df)
    df = compute_multiple_moving_averages(df, windows=sma_windows)
    df = compute_multiple_exponential_moving_averages(df, windows=ema_windows)
    for window in bollinger_windows:
        df = compute_bollinger_bands(df, window=window)
    for window in rsi_windows:
        df = compute_rsi(df, window=window)
    for window in atr_windows:
        df = compute_atr(df, window=window)
    df = compute_order_flow_imbalance(df)
    df = compute_volume_delta(df)

    # Drop NaNs created by rolling calculations
    df.dropna(inplace=True)

    # Save data with features
    df.to_csv(feature_file_path, index=False)
    print(f" Feature-engineered data saved to {feature_file_path}")

    return df

# Example Usage:
process_features("BTCUSDT", sma_windows=[50, 100, 200], ema_windows=[10, 20, 50], rsi_windows=[14, 21])
