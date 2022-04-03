from pandas_datareader import data as pdr
import yfinance as yf
import pandas as pd
import datetime
import numpy as np

yf.pdr_override()

#Fetch Tickers Data
get_tickers = pd.read_csv('Current-Holdings.csv')
tickers = list(get_tickers['Symbol'])

# Variables
start_date = datetime.datetime.now() - datetime.timedelta(days=1000)
end_date = datetime.date.today()
volatility_window10 = 10
volatility_window5 = 5
Percentile_rankwindow13w = 91
Percentile_rankwindow26w = 182
Percentile_rankwindow52w = 252

master_df = pd.DataFrame(columns=['Ticker', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume',
       'dailyreturns', 'volatility10', 'volatility5',
       'Percentile Volatility (10) 13w', 'Percentile Volatility (10) 26w',
       'Percentile Volatility (10) 52w', 'Percentile Volatility (5) 13w',
       'Percentile Volatility (5) 26w', 'Percentile Volatility (5) 52w',
       'Percentile Volume 13w', 'Percentile Volume 26w',
       'Percentile Volume 52w'])

counter = 0

for ticker in tickers:
    # Download historical data as CSV for each stock (makes the process faster)
    df = pdr.get_data_yahoo(ticker, start_date, end_date)

    # compute volatility using Pandas rolling and std methods, the trading days is set to 1000 days
    returns = np.log(df['Close']/df['Close'].shift(1))
    returns.fillna(0, inplace=True)
    df['dailyreturns'] = returns
    volatility10 = returns.rolling(window=volatility_window10).std()
    volatility5 = returns.rolling(window=volatility_window5).std()
    df['volatility10'] = volatility10
    df['volatility5'] = volatility5

    # create a new column of percentile rank
    df['Percentile Volatility (10) 13w'] = df.volatility10.tail(Percentile_rankwindow13w).rank(pct=True)
    df['Percentile Volatility (10) 26w'] = df.volatility10.tail(Percentile_rankwindow26w).rank(pct=True)
    df['Percentile Volatility (10) 52w'] = df.volatility10.tail(Percentile_rankwindow52w).rank(pct=True)

    df['Percentile Volatility (5) 13w'] = df.volatility5.tail(Percentile_rankwindow13w).rank(pct=True)
    df['Percentile Volatility (5) 26w'] = df.volatility5.tail(Percentile_rankwindow26w).rank(pct=True)
    df['Percentile Volatility (5) 52w'] = df.volatility5.tail(Percentile_rankwindow52w).rank(pct=True)

    df['Percentile Volume 13w'] = df.Volume.tail(Percentile_rankwindow13w).rank(pct=True)
    df['Percentile Volume 26w'] = df.Volume.tail(Percentile_rankwindow26w).rank(pct=True)
    df['Percentile Volume 52w'] = df.Volume.tail(Percentile_rankwindow52w).rank(pct=True)

    df.insert(0, 'Ticker', ticker)

    #Only grabbing the last line of each dataframe.
    master_df = pd.concat([master_df, df.tail(1)])
    print(f'Processed {counter} files currently')
    counter += 1

master_df.to_excel('output.xlsx')








