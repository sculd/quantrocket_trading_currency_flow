# Copyright 2020 QuantRocket LLC - All Rights Reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from moonshot import Moonshot
from moonshot.commission import PerShareCommission
from quantrocket.master import get_securities
import pandas as pd


name_to_index_sid_databases = {
    "china": ("FIBBG006H1RJZ6", ""), "canada": ("FIBBG000QW7RC0", ""), 
    "japan": ("FIBBG009S0XQY8", ""), "mexico": ("FIBBG0015XN496", "london-1d"), 
    #"hungary": ("FIBBG000QGWGG7", "hungary-1d"), # not yet found the one in USD currency
    #"sweden": ("FIBBG000QZXB02", "mexico-1d"), # not yet found the one in the USD currency
    "poland": ("FIBBG001DQBCC3", "london-1d"), 
    #"korea": ("FIBBG000DPT7D8", ""), 
    "thailand": ("FIBBG0017DVJR6", "london-1d"), 
    "newzealand": ("FIBBG0015M9W30", "nasdaq-1d"), 
    #"hongkong": ("FIBBG007V5QTW1", "china-1d"), # hkd is pegged to usd thus excluded
}

name_to_currency_sids = {
    "australia": "FXAUDUSD",
    "china": "FXUSDCNH", # china
    "newzealand": "FXNZDUSD", #newzeland
    "norway": "FXUSDNOK", # norway
    "canada": "FXUSDCAD", # canada
    "japan": "FXUSDJPY", # japan
    "thailand": "FXUSDTHB",
    "swiss": "FXUSDCHF", # swiss
    "turkey": "FXUSDTRY", # turkey
    "poland": "FXUSDPLN", # poland
    "singapore": "FXUSDSGD",
    "zecko": "FXUSDCZK",
    "denmark": "FXUSDDKK", # denmark
    "hungary": "FXUSDHUF", # hungary
    "eu": "FXEURUSD",
    "england": "FXGBPUSD", # uk
    "mexico": "FXUSDMXN", # mexico
    "hongkong": "FXUSDHKD",
    "sweden": "FXUSDSEK", # sweden
    "southafrica": "FXUSDZAR",
}
sids_cash = list(get_securities(sec_types="CASH").reset_index()['Sid'].values)
name_to_currency_sids = {name: sid for name, sid in name_to_currency_sids.items() if sid in sids_cash}

name_to_index_sids = {name: index_sid_db[0] for name, index_sid_db in name_to_index_sid_databases.items()}
name_to_index_sids = {name: sid for name, sid in name_to_index_sids.items() if name in name_to_currency_sids}

index_sids = list(name_to_index_sids.values())

index_sid_to_fx_sids = {
    index_sid: name_to_currency_sids[name] for name, index_sid in name_to_index_sids.items() if name in name_to_currency_sids
}

sid_snp500 = "FIBBG003MVLMY1"

def get_return(return_period_days, return_delay_days, price):
    price_past = price.shift(return_period_days)
    returns = (price - price_past) / price_past
    returns = returns.fillna(method="ffill")
    return returns.shift(return_delay_days)


def sort_index_returns(beta_days, gamma_days, price_ind):
    return_ind = get_return(beta_days, gamma_days, price_ind).iloc[-1]

    return_inds = {}
    for name, sid in name_to_index_sids.items():
        if sid not in return_ind:
            continue
        return_inds[name] = [return_ind[sid]]

    df_return_inds = pd.DataFrame.from_dict(return_inds)
    df_return_inds = df_return_inds.sort_values(by=0, axis=1)
    if len(return_inds) == 0:
            return [], df_return_inds
    return df_return_inds.columns.values, df_return_inds


class TradingCurrencyFlow(Moonshot):
    """
    Strategy that buys recent winners and sells recent losers.

    Specifically:

    - rank stocks by their performance over the past MOMENTUM_WINDOW days
    - ignore very recent performance by excluding the last RANKING_PERIOD_GAP
    days from the ranking window (as commonly recommended for UMD)
    - buy the TOP_N_PCT percent of highest performing stocks and short the TOP_N_PCT
    percent of lowest performing stocks
    - rebalance the portfolio according to REBALANCE_INTERVAL
    """

    CODE = "moonshot_trading_currency_flow"
    
    ALPHA_DAYS = 10
    BETA_DAYS = 20
    GAMMA_DAYS = 10
    LONG_SIZE = 2
    SHORT_SIZE = 2
    
    MOMENTUM_WINDOW = 252 # rank by twelve-month returns
    RANKING_PERIOD_GAP = 22 # but exclude most recent 1 month performance
    TOP_N_PCT = 50 # Buy/sell the top/bottom 50%
    REBALANCE_INTERVAL = "M" # M = monthly; see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases

    def prices_to_signals(self, prices):
        """
        This method receives a DataFrame of prices and should return a
        DataFrame of integer signals, where 1=long, -1=short, and 0=cash.
        """
        df_close = prices.loc["Close"]
        print(f"df_close:\n{df_close}")
        
        columns_fx = [c for c in df_close.columns if 'FX' in c]
        
        df_ind = df_close[index_sids]
        print(f"df_ind:\n{df_ind}")
        
        df_fx = df_close[columns_fx]
        print(f"df_fx:\n{df_fx}")
        
        df_return_ind = get_return(self.BETA_DAYS, self.GAMMA_DAYS, df_ind)
        df_return_fx = df_return_ind.rename(columns=index_sid_to_fx_sids)
        print(f"df_return_ind:\n{df_return_ind}")
        print(f"df_return_fx:\n{df_return_fx}")
        
        df_return_fx_ranks = df_return_fx.rank(axis=1, ascending=False, pct=True)
        print(f"df_return_fx_ranks:\n{df_return_fx_ranks}")
        longs = (df_return_fx_ranks <= 0.25)
        shorts = (df_return_fx_ranks > 1 - 0.25)
        longs = longs.astype(int)
        shorts = -shorts.astype(int)
        signals = longs.where(longs == 1, shorts)
        signals = signals.rename(columns=name_to_currency_sids)
        print(f"longs:\n{longs}")
        print(f"shorts:\n{shorts}")
        print(f"signals:\n{signals}")
        
        '''
        price_ind = df_close[index_sids]
        ind_sorted_names, df_return_inds = sort_index_returns(self.BETA_DAYS, self.GAMMA_DAYS, price_ind)
        print(f"ind_sorted_names:\n{ind_sorted_names}")
        print(f"df_return_inds:\n{df_return_inds}")
        df_return_inds_ranks = df_return_inds.rank(axis=1, ascending=False, pct=True)
        print(f"df_return_inds_ranks:\n{df_return_inds_ranks}")
        longs = (df_return_inds_ranks <= 0.25)
        shorts = (df_return_inds_ranks > 1 - 0.25)
        longs = longs.astype(int)
        shorts = -shorts.astype(int)
        signals = longs.where(longs == 1, shorts)
        signals = signals.rename(columns=name_to_currency_sids)
        print(f"longs:\n{longs}")
        print(f"shorts:\n{shorts}")
        print(f"signals:\n{signals}")
        #'''

        '''
        # Calculate the returns
        returns = df_close.shift(self.RANKING_PERIOD_GAP)/df_close.shift(self.MOMENTUM_WINDOW) - 1
        print(f"returns.columns: \n{returns.columns}")

        # Rank the best and worst
        top_ranks = returns.rank(axis=1, ascending=False, pct=True)
        bottom_ranks = returns.rank(axis=1, ascending=True, pct=True)
        print(f"returns: \n{returns}")
        print(f"top_ranks: \n{top_ranks}")
        print(f"bottom_ranks: \n{bottom_ranks}")
        print(f"returns.iloc[-1]: \n{returns.iloc[-1]}")
        print(f"top_ranks.iloc[-1]: \n{top_ranks.iloc[-1]}")
        print(f"bottom_ranks.iloc[-1]: \n{bottom_ranks.iloc[-1]}")

        top_n_pct = self.TOP_N_PCT / 100

        # Get long and short signals and convert to 1, 0, -1
        longs = (top_ranks <= top_n_pct)
        shorts = (bottom_ranks <= top_n_pct)
        
        longs = longs.astype(int)
        shorts = -shorts.astype(int)

        # Combine long and short signals
        signals = longs.where(longs == 1, shorts)

        print(f"longs: \n{longs}")
        print(f"signals: \n{signals}")

        # Resample using the rebalancing interval.
        # Keep only the last signal of the month, then fill it forward
        signals = signals.resample(self.REBALANCE_INTERVAL).last()
        print(f"signals after resample: \n{signals}")
        signals = signals.reindex(df_close.index, method="ffill")
        print(f"signals after reindex: \n{signals}")
        #'''

        return signals


    def signals_to_target_weights(self, signals, prices):
        """
        This method receives a DataFrame of integer signals (-1, 0, 1) and
        should return a DataFrame indicating how much capital to allocate to
        the signals, expressed as a percentage of the total capital allocated
        to the strategy (for example, -0.25, 0, 0.1 to indicate 25% short,
        cash, 10% long).
        """
        weights = self.allocate_equal_weights(signals)
        print(f"weights: \n{weights}")
        return weights

    def target_weights_to_positions(self, weights, prices):
        """
        This method receives a DataFrame of allocations and should return a
        DataFrame of positions. This allows for modeling the delay between
        when the signal occurs and when the position is entered, and can also
        be used to model non-fills.
        """
        # Enter the position in the period/day after the signal
        print(f"weights.shift(): \n{weights.shift()}")
        return weights.shift()

    def positions_to_gross_returns(self, positions, prices):
        """
        This method receives a DataFrame of positions and a DataFrame of
        prices, and should return a DataFrame of percentage returns before
        commissions and slippage.
        """
        # Our return is the security's close-to-close return, multiplied by
        # the size of our position. We must shift the positions DataFrame because
        # we don't have a return until the period after we open the position
        df_close = prices.loc["Close"]
        columns_fx = [c for c in df_close.columns if 'FX' in c]
        df_fx = df_close[columns_fx]
        df_fx = df_close[positions.columns]
        print(f"positions.columns: \n{positions.columns}")
        print(f"df_fx.columns: \n{df_fx.columns}")
        print(f"positions: \n{positions}")
        gross_returns = df_fx.pct_change() * positions.shift()
        print(f"gross_returns: \n{gross_returns}")
        return gross_returns

class USStockCommission(PerShareCommission):
    BROKER_COMMISSION_PER_SHARE = 0.005

class TradingCurrencyFlowDemo(TradingCurrencyFlow):

    CODE = "moonshot_trading_currency_flow"
    DB = ["london-1d", "nasdaq-1d", "fx-1d"]
    #DB = ["fx-1d"]
    TIMEZONE = "Europe/London"
    #UNIVERSES = "global-indiced-1d"
    #UNIVERSES = "fx-1d"
    UNIVERSES = "trading-currency-flows-1d-1"
    TOP_N_PCT = 50
    COMMISSION_CLASS = USStockCommission



