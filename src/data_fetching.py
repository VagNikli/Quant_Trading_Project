# Data fetching module
import os
import time
import requests
import pandas as pd
import os
import time
import requests
import pandas as pd
import matplotlib.pyplot as plt

def fetch_crypto_data_binance(symbols=["BTCUSDT"], interval="1h", start_date="2021-01-01", end_date="2023-03-01", save_path="./data/crypto_data", output_path="./outputs"):
    """
    Fetch historical crypto data from Binance public API.
    Generates and saves closing price plots for multiple cryptocurrencies.
    """

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    print(f"Fetching data for {', '.join(symbols)} from Binance public API...")

    start_timestamp = int(pd.Timestamp(start_date).timestamp() * 1000)
    end_timestamp = int(pd.Timestamp(end_date).timestamp() * 1000)
    url = "https://api.binance.com/api/v3/klines"

    for symbol in symbols:
        print(f"Fetching data for {symbol}...")
        params = {
            "symbol": symbol,
            "interval": interval,
            "startTime": start_timestamp,
            "endTime": end_timestamp,
            "limit": 1000  # Max limit per request
        }

        all_data = []
        last_fetched_timestamp = start_timestamp

        while last_fetched_timestamp < end_timestamp:
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()  # Handle errors
                
                data = response.json()
                if not data:
                    print(f"No more data returned for {symbol}.")
                    break

                all_data.extend(data)
                last_fetched_timestamp = data[-1][0] + 1  # Move forward
                params["startTime"] = last_fetched_timestamp

                time.sleep(0.5)  # Respect API rate limits

            except requests.exceptions.RequestException as e:
                print(f"Error fetching data for {symbol}: {e}")
                break

        if not all_data:
            print(f"No data fetched for {symbol}. Skipping...")
            continue

        # Define column names based on Binance Kline structure
        columns = ["Timestamp", "Open", "High", "Low", "Close", "Volume", "CloseTime", "QuoteAssetVolume", 
                   "NumberOfTrades", "TakerBuyBaseVolume", "TakerBuyQuoteVolume", "Ignore"]
        df = pd.DataFrame(all_data, columns=columns)

        # Convert timestamp and select useful columns
        df["Timestamp"] = pd.to_datetime(df["Timestamp"], unit="ms")
        numeric_cols = ["Open", "High", "Low", "Close", "Volume", "QuoteAssetVolume", 
                        "NumberOfTrades", "TakerBuyBaseVolume", "TakerBuyQuoteVolume"]
        df[numeric_cols] = df[numeric_cols].astype(float)
        df = df[["Timestamp", "Open", "High", "Low", "Close", "Volume", 
                 "QuoteAssetVolume", "NumberOfTrades", "TakerBuyBaseVolume", "TakerBuyQuoteVolume"]]

        # Save the dataset
        file_path = os.path.join(save_path, f"{symbol}_binance_data.csv")
        df.to_csv(file_path, index=False)
        print(f" Data for {symbol} saved to {file_path}")

        # Plot Close Prices and save for each symbol
        plt.figure(figsize=(12, 6))
        plt.plot(df["Timestamp"], df["Close"], label=f"{symbol} Close Price", color="blue")
        plt.xlabel("Date")
        plt.ylabel("Close Price (USD)")
        plt.title(f"{symbol} Close Prices Over Time")
        plt.legend()
        plt.grid()
        
        plot_path = os.path.join(output_path, f"{symbol}_close_prices.png")
        plt.savefig(plot_path)
        plt.close()
        print(f" {symbol} Close Price Plot saved to {plot_path}")

    return True


# Example crypto:
fetch_crypto_data_binance(symbols=["BTCUSDT"], interval="1h", start_date="2021-01-01", end_date="2024-03-01")
