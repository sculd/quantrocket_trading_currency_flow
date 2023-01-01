import zipline.api as algo
from zipline.finance.execution import MarketOrder

MOMENTUM_WINDOW = 252
ALPHA_DAYS = 10
BETA_DAYS = 20
GAMMA_DAYS = 10
LONG_SIZE = 2
SHORT_SIZE = 2

def initialize(context):
    """
    Called once at the start of a backtest, and once per day in
    live trading.
    """
    
    context.name_to_index_sids = {
         "china": "FIBBG00203J8V6", "canada": "FIBBG0029T2KJ5", 
         "japan": "FIBBG000BL97R6", "mexico": "FIBBG0015XN496", 
         "hungary": "FIBBG000QGWGG7", "sweden": "FIBBG000QZXB02", 
         "poland": "FIBBG001CGQZG5", "korea": "FIBBG000PQY818", 
         "thailand": "FIBBG0017DVJR6", "newzealand": "FIBBG001CGQZJ2", 
         "hongkong": "FIBBG007V5QTW1",
    }


    context.name_to_currency_sids = {
        "australia": "FXAUDUSD",
        "china": "FXUSDCNH", # china
        "newzeland": "FXNZDUSD", #newzeland
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

    # SPY (i believe)
    algo.set_benchmark(algo.sid('FIBBG000BDTBL9'))

    # Rebalance every day, 30 minutes before market close.
    algo.schedule_function(
        rebalance,
        algo.date_rules.every_day(),
        algo.time_rules.market_close(minutes=30),
    )


def before_trading_start(context, data):
    """
    Called every day before market open.
    """
    pass


def rebalance(context, data):
    # update the long and short list
    ind_sorted_names, df_return_inds = sort_index_returns(context, data)
    sids_long = ind_sorted_names[-LONG_SIZE:]
    sids_short = ind_sorted_names[:SHORT_SIZE]

    positions = context.portfolio.positions

    # Exit positions we no longer want to hold
    for asset, position in positions.items():
        if asset not in sids_long:
            algo.order_target_value(asset, 0, style=MarketOrder())

    # Enter long positions
    for asset in sids_long:

        # if already long, nothing to do
        if asset in positions:
            continue

        # otherwise, buy a fixed $100K position per asset
        algo.order_target_value(asset, 100e3, style=MarketOrder())


def get_return(return_period_days, return_delay_days, price):
    price_past = price.shift(return_period_days)
    returns = (price - price_past) / price_past
    returns = returns.fillna(method="ffill")
    return returns.shift(return_delay_days)


def sort_index_returns(context, data):
    price_ind = data.history(list(context.name_to_index_sids.keys()), "close", BETA_DAYS + GAMMA_DAYS, "1d")
    return_ind = get_return(BETA_DAYS, GAMMA_DAYS, price_ind).iloc[-1]

    return_inds = {}
    for name, sid in context.name_to_index_sids.items():
        if sid not in return_ind:
            continue
        return_inds[name] = [return_ind[sid]]

    df_return_inds = pd.DataFrame.from_dict(return_inds)
    ind_sorted_names = df_return_inds.sort_values(by=0, axis=1).columns.values
    return ind_sorted_names, df_return_inds

