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
from moonshot.commission import PercentageCommission
from quantrocket.master import get_securities
import pandas as pd


sid_snp500 = "FIBBG000BDTBL9" # "FIBBG003MVLMY1"

def if_fx_sid_has_ind_sid(fx_sid):
    for name, sid in name_to_currency_sids.items():
        if fx_sid != sid:
            continue
        return name in name_to_index_sids


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

    - rebalance the portfolio according to REBALANCE_INTERVAL
    """

    CODE = "moonshot_trading_currency_flow"
    
    ALPHA_DAYS = 5
    
    REBALANCE_INTERVAL = "W" # M = monthly; see https://pandas.pydata.org/pandas-docs/stable/user_guide/timeseries.html#offset-aliases

    def prices_to_signals(self, prices):
        """
        This method receives a DataFrame of prices and should return a
        DataFrame of integer signals, where 1=long, -1=short, and 0=cash.
        """
        df_close = prices.loc["Close"]
        print(f"df_close:\n{df_close}")
        print(f"df_close.columns:\n{df_close.columns}")
        
        columns_fx = [c for c in df_close.columns if 'FX' in c]
        
        df_fx = df_close[columns_fx]
        print(f"df_fx:\n{df_fx}")
        
        df_return_fx = get_return(self.ALPHA_DAYS, 0, df_fx)
        print(f"df_return_fx:\n{df_return_fx}")
        
        # Rank the best and worst
        top_ranks = df_return_fx.rank(axis=1, ascending=False, pct=True)
        bottom_ranks = df_return_fx.rank(axis=1, ascending=True, pct=True)

        top_n_pct = self.TOP_N_PCT / 100

        # Get long and short signals and convert to 1, 0, -1
        longs = (top_ranks <= top_n_pct)
        shorts = (bottom_ranks <= top_n_pct)

        longs = longs.astype(int)
        shorts = -shorts.astype(int)

        # Combine long and short signals
        #signals = longs.where(longs == 1, shorts)
        signals = -shorts

        # Resample using the rebalancing interval.
        # Keep only the last signal of the month, then fill it forward
        signals = signals.resample(self.REBALANCE_INTERVAL).last()
        signals = signals.reindex(df_close.index, method="ffill")

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
        print(f"weights: \n{weights.tail(10)}")
        return weights

    def target_weights_to_positions(self, weights, prices):
        """
        This method receives a DataFrame of allocations and should return a
        DataFrame of positions. This allows for modeling the delay between
        when the signal occurs and when the position is entered, and can also
        be used to model non-fills.
        """
        # Enter the position in the period/day after the signal
        print(f"weights.shift(): \n{weights.shift().tail(10)}")
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
        print(f"positions: \n{positions.tail(10)}")
        gross_returns = df_fx.pct_change() * positions.shift()
        print(f"gross_returns: \n{gross_returns.tail(10)}")
        return gross_returns

class USStockCommission(PercentageCommission):
    BROKER_COMMISSION_RATE = 0.0005 # 0.05% of trade value

class TradingCurrencyFlowDemo(TradingCurrencyFlow):

    CODE = "currency_momentum"
    DB = ["london-1d", "nasdaq-1d", "fx-1d", "usstock-1d"]
    #DB = ["fx-1d"]
    TIMEZONE = "Europe/London"
    UNIVERSES = "fx-1d"
    TOP_N_PCT = 25
    COMMISSION_CLASS = USStockCommission



