import sqlite3
from datetime import datetime, date
from price_loaders.tradingview import load_asset_price
import pandas as pd

def futures_database_creation():
    con = sqlite3.connect('futures.db')
    cur = con.cursor()

    ag_tables_colnames = """
        time DATE,
        exp_month TEXT,
        exp_year INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        market_year TEXT,

        PRIMARY KEY (time, exp_month, exp_year)
    """

    macro_tables_colnames = """
        time DATE,
        exp_month TEXT,
        exp_year INTEGER,
        open REAL,
        high REAL,
        low REAL,
        close REAL,

        PRIMARY KEY (time, exp_month, exp_year)
    """

    # AGRICULTURAL COMMODITIES FUTURES DATA
        # CORN
    cur.execute(f"CREATE TABLE IF NOT EXISTS CBOT_corn({ag_tables_colnames})")
    cur.execute(f"CREATE TABLE IF NOT EXISTS B3_corn({ag_tables_colnames})")

        # SOYBEAN AND ITS PRODUCTS
    cur.execute(f"CREATE TABLE IF NOT EXISTS CBOT_soybean({ag_tables_colnames})")
    cur.execute(f"CREATE TABLE IF NOT EXISTS CBOT_soybean_oil({ag_tables_colnames})")
    cur.execute(f"CREATE TABLE IF NOT EXISTS CBOT_soybean_meal({ag_tables_colnames})")

    # MACROECONOMICAL FUTURES DATA
        # US
    cur.execute(f"CREATE TABLE IF NOT EXISTS US_SOFR({macro_tables_colnames})")
    cur.execute(f"CREATE TABLE IF NOT EXISTS US_DXY({macro_tables_colnames})")

        # BRAZIL
    cur.execute(f"CREATE TABLE IF NOT EXISTS BR_DI1({macro_tables_colnames})")
    cur.execute(f"CREATE TABLE IF NOT EXISTS BR_DOL({macro_tables_colnames})")

    cur.close()

def market_year_flag(exp_year, exp_month):
    """
        Returns a market-year flag for corn and soybeans futures contracts.

        Args
        ------
        exp_year: int - Expire year of the contract;
        exp_month: str - Expire month of the contract.

        Returns
        -------
        market-year: str - A flag to identify the market-year for each contract.
    """
    exp_month_to_num = {
        'F':  1,
        'G':  2,
        'H':  3,
        'J':  4,
        'K':  5,
        'M':  6,
        'N':  7,
        'Q':  8,
        'U':  9,
        'V': 10,
        'X': 11,
        'Z': 12
    }

    my_beg = exp_month_to_num['U']  # To these commodities, the market-year starts at 01-September
    my_end = exp_month_to_num['Q']  # and ends at 31-Aug.

    c_exp_month = exp_month_to_num[exp_month]

    if (c_exp_month >= my_beg):
        return f'{exp_year}/{str(exp_year+1)[-2:]}'
    elif (c_exp_month <= my_end):
        return f'{exp_year-1}/{str(exp_year)[-2:]}'


def scrap_tw(asset_name: str, expire_months: list, init_year: int, period: int, lookback: int):
    """
        Scraps TradingView data using the load_asset_price function from price_loaders package.

        Args
        ------
        asset_name: str - Prefix string for the futures contracts of the underlying asset;
        expire_months: list - A list with the expire months available;
        init_year: int - Initial year;
        period: int - How many days of each contracs will be storaged;
        lookback: int - How many expire years will be storaged.

        Returns
        --------
        pd.DataFrame with columns:
            time: date;
            expire_month: str;
            expire_year: int;
            open: float;
            high: float;
            low; float;
            close; float

            if it's a agricultural commodity asset, it'll also have a colums...
                market-year: str.

    """
    dfs = []
    for i in range(lookback):
        for m in expire_months:
            ticker = f'{asset_name}{m}{init_year-i}'
            try:
                df = load_asset_price(ticker, period, 'D')
                if df.empty:
                    return print(f"No data for {ticker}")
                    continue
                
                df['time'] = df['time'].dt.tz_convert('America/Sao_Paulo')
                df['time'] = df['time'].apply(lambda x: datetime(x.year, x.month, x.day))

                df['exp_month'] = m
                df['exp_year'] = init_year - i

                if asset_name in ['ZC', 'CCM', 'ZS', 'ZL', 'ZM']:
                    df['market_year'] = market_year_flag(init_year - i, m)

                dfs.append(df)
            
            except Exception as e:
                print(f"Error scrapping data for {ticker}.")
                continue
    
    scrap = pd.concat(dfs)
    common_cols = ['time', 'exp_month', 'exp_year', 'open', 'high', 'low', 'close']
    if asset_name in ['ZC', 'CCM', 'ZS', 'ZL', 'ZM']:
        return scrap[common_cols + ['market_year']]
    else:
        return scrap[common_cols]

def download_futures_prices():
    # Standard variables
    INIT_YEAR = datetime.now().year+1
    PERIOD = 10_000
    LOOKBACK = 10

    con = sqlite3.connect('futures.db')

    assets_expire_months = {
        # AGRICULTURAL DATA
            # Corn
        'ZC': [['H', 'K', 'N', 'U', 'Z'], "CBOT_corn"],
        'CCM': [['F','H', 'K', 'N', 'U', 'X'], "B3_corn"],

            # Soybeans and related
        'ZS': [['F', 'H', 'K', 'N', 'Q', 'U', 'X'], "CBOT_soybeans"],
        'ZL': [['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'], "CBOT_soybean_oil"],
        'ZM': [['F', 'H', 'K', 'N', 'Q', 'U', 'V', 'Z'], "CBOT_soybeans_meal"],

        # MACRO DATA
            # Brazil
        'DOL': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], 'BR_DOL'],
        'DI1': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], 'BR_DI1'],

            # United States
        'DX': [['H', 'M', 'U', 'Z'], 'US_DXY'],
        'SF1': [['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z'], 'US_SOFR']
    }

    for asset in assets_expire_months.keys():
        exp_months = assets_expire_months[asset][0]
        table_name = assets_expire_months[asset][1]

        try: 
            scrapped_data = scrap_tw(
                asset,
                exp_months,
                INIT_YEAR,
                PERIOD,
                LOOKBACK
            )
        except Exception as e:
            print(f"Unexpected error ocurre while scrapping {asset}: {e}")

        try: 
            scrapped_data.to_sql(table_name, con, if_exists='append', index = False)
            print(f"Successfully inserted {len(scrapped_data)} records into {table_name}.")
        except Exception as e:
            print(f"An unexpected error occurred during DB insertion for {table_name}: {e}")

if __name__ == '__main__':
    # 1. Ensure the database structure exists
    futures_database_creation()
    
    # 2. Download and insert the data
    download_futures_prices()
