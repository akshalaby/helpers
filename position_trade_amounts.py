import random

import pandas as pd


def fifo_trade_amounts(df):
    """Returns the times and amounts corresponding to the latest position of the give df using FIFO accounting

    Args:
        df (pd.DataFrame): A dataframe with 'time', 'amount', and 'pos' columns

    Returns:
        pd.DataFrame: A dataframe with 'time', 'amount' that contributed to latest position
    """
    # Transformations require the data is sorted
    df.sort_values("time")

    # Get latest position
    final_pos = df["pos"].iloc[-1]

    # Filter out any trades before the position was opened/flipped
    # If long, only consider trades after a negative/zero position
    # If short, only consider trades after a positive/zero position
    df["keep"] = None
    if final_pos > 0:
        df.loc[df["pos"] <= 0, "keep"] = False
    else:
        df.loc[df["pos"] >= 0, "keep"] = False
    df["keep"] = df["keep"].bfill()
    df["keep"] = df["keep"].fillna(True)
    df = df[df["keep"]]

    # With FIFO, we want all the latest trades in the direction of our position
    # that sum up to our position.  We do this by:

    # 1. Filter out trades that oppose latest position
    # If we are long, only consider buys
    # If we are short, only consider sells
    if final_pos > 0:
        df = df[df["amount"] > 0]
    else:
        df = df[df["amount"] < 0]

    # 2. Compute running sum (backwards) to find cutoff for most recent trades
    # contributing to current position.
    df["sum_amount"] = df.loc[::-1, "amount"].cumsum(skipna=True)[::-1]

    # 3. Filter the rows where the sum of trades has exceeded the current
    # position
    df["sum_amount_shift"] = df["sum_amount"].shift(-1, fill_value=0)
    if final_pos > 0:
        df = df[df["sum_amount_shift"] <= final_pos]
    else:
        df = df[df["sum_amount_shift"] >= final_pos]

    # Compute overage in case only a portion of the trade at the boundary
    # applies
    df["overage"] = df["sum_amount"] - final_pos
    if final_pos > 0:
        df.loc[df["overage"] < 0, "overage"] = 0
    else:
        df.loc[df["overage"] > 0, "overage"] = 0

    # Remove the overage from the trade amount
    df["amount"] = df["amount"] - df["overage"]

    # Filter out any non-contributing rows
    df = df[df["amount"] != 0]

    # Filter columns to time and amount
    df = df[["time", "amount"]]
    return df


def lifo_trade_amounts(df):
    """Returns the times and amounts corresponding to the latest position of the give df using LIFO accounting

    Args:
        df (pd.DataFrame): A dataframe with 'time', 'amount', and 'pos' columns

    Returns:
        pd.DataFrame: A dataframe with 'time', 'amount' corresponding to latest position
    """
    # Transformations require the data is sorted
    df.sort_values("time")

    # Get latest position
    final_pos = df["pos"].iloc[-1]

    # Filter out any trades before the position was opened/flipped
    # If long, only consider trades after a negative/zero position
    # If short, only consider trades after a positive/zero position
    df["keep"] = None
    if final_pos > 0:
        df.loc[df["pos"] <= 0, "keep"] = False
    else:
        df.loc[df["pos"] >= 0, "keep"] = False
    df["keep"] = df["keep"].bfill()
    df["keep"] = df["keep"].fillna(True)
    df = df[df["keep"]]

    # With LIFO any trades opposing the position eat up prior trades.  This
    # means we can only consider the running minimum positon (in absolute
    # value) looking back. Trades outside the minimum absolute value are
    # filtered out since they're used up by later trades.
    if final_pos > 0:
        # Take the running min of position walking backwards in time
        df.loc[:, "min_pos"] = df.loc[::-1, "pos"].cummin()[::-1]

        # Filter out trades that oppose latest position
        df = df[df["amount"] > 0]

        # Account for trade amounts that flip the position. We only want to
        # consider the amount in the long direction.
        df["amount"] = df[["amount", "pos"]].min(axis=1)
    else:
        # Take the running max on pos walking backwards in time
        df.loc[:, "min_pos"] = df.loc[::-1, "pos"].cummax()[::-1]

        # Filter out trades that oppose latest position
        df = df[df["amount"] < 0]

        # Account for trade amounts that flip the position. We only want to
        # consider the amount in the short direction.
        df["amount"] = df[["amount", "pos"]].max(axis=1)

    df["overage"] = df["pos"] - df["min_pos"]
    df["amount"] = df["amount"] - df["overage"]

    # Filter out any trades that have been used up or are against our position
    if final_pos > 0:
        df = df[df["amount"] > 0]
    else:
        df = df[df["amount"] < 0]

    # Filter columns to time and amount
    df = df[["time", "amount"]]
    return df
