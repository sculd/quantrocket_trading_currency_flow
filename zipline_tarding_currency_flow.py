# Copyright 2022 QuantRocket LLC - All Rights Reserved
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

import zipline.api as algo
from zipline.pipeline import Pipeline
from zipline.pipeline.factors import AverageDollarVolume, Returns
from zipline.finance.execution import MarketOrder

MOMENTUM_WINDOW = 252
ALPHA_DAYS = 10
BETA_DAYS = 20
GAMMA_DAYS = 10

def initialize(context):
    """
    Called once at the start of a backtest, and once per day in
    live trading.
    """
    
    context.sids_index = \
         {"china": "FIBBG00203J8V6", "canada": "FIBBG0029T2KJ5", 
         "japan": "FIBBG000BL97R6", "mexico": "FIBBG0015XN496", 
         "hungary": "FIBBG000QGWGG7", "sweden": "FIBBG000QZXB02", 
         "poland": "FIBBG001CGQZG5", "korea": "FIBBG000PQY818", 
         "thailand": "FIBBG0017DVJR6", "newzealand": "FIBBG001CGQZJ2", 
         "hongkong": "FIBBG007V5QTW1"}
    
    # Attach the pipeline to the algo
    algo.attach_pipeline(make_pipeline(), 'pipeline')

    algo.set_benchmark(algo.sid('FIBBG000BDTBL9'))

    # Rebalance every day, 30 minutes before market close.
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(minutes=30),
    )

def make_pipeline():
    """
    Create a pipeline that filters by dollar volume and
    calculates return.
    """
    pipeline = Pipeline(
        columns={
            "returns": Returns(window_length=MOMENTUM_WINDOW),
        },
        screen=AverageDollarVolume(window_length=30) > 10e6
    )
    return pipeline

def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    factors = algo.pipeline_output('pipeline')

    # Get the top 3 stocks by return
    returns = factors["returns"].sort_values(ascending=False)
    context.winners = returns.index[:3]

def rebalance(context, data):
    """
    Execute orders according to our schedule_function() timing.
    """

    # calculate intraday returns for our winners
    current_prices = data.current(context.winners, "price")
    prior_closes = data.history(context.winners, "close", 2, "1d").iloc[0]
    intraday_returns = (current_prices - prior_closes) / prior_closes

    positions = context.portfolio.positions

    # Exit positions we no longer want to hold
    for asset, position in positions.items():
        if asset not in context.winners:
            algo.order_target_value(asset, 0, style=MarketOrder())

    # Enter long positions
    for asset in context.winners:

        # if already long, nothing to do
        if asset in positions:
            continue

        # if the stock is up for the day, don't enter
        if intraday_returns[asset] > 0:
            continue

        # otherwise, buy a fixed $100K position per asset
        algo.order_target_value(asset, 100e3, style=MarketOrder())

def sort_index_returns(context, data):
    price_ind = data.history(list(context.sids_index.keys()), "close", BETA_DAYS + GAMMA_DAYS, "1d").iloc[0]
    price_ind_past = price_ind.shift(ALPHA_DAYS)
    return_ind = (price_ind - price_ind_past) / price_ind_past

    return_inds = {}
    for name, sid in context.sids_index.items():
        if sid not in return_ind:
            continue
        return_inds[name] = [return_ind[sid].iloc[-1]]

    df_return_inds = pd.DataFrame.from_dict(return_inds)
    ind_sorted_names = df_return_inds.sort_values(by=0, axis=1).columns.values
    return ind_sorted_names, df_return_inds
