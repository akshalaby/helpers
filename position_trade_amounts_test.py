import random
import time

import pandas as pd

from position_trade_amounts import fifo_trade_amounts, lifo_trade_amounts

TEST_CASES = [
    {
        "trades": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-03-01", 1)],
        "fifo": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-03-01", 1)],
        "lifo": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-03-01", 1)],
    },
    {
        "trades": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-03-01", -1)],
        "fifo": [("2020-02-01", 1)],
        "lifo": [("2020-01-01", 1)],
    },
    {
        "trades": [("2020-01-01", 2), ("2020-02-01", 2), ("2020-03-01", 2)],
        "fifo": [("2020-01-01", 2), ("2020-02-01", 2), ("2020-03-01", 2)],
        "lifo": [("2020-01-01", 2), ("2020-02-01", 2), ("2020-03-01", 2)],
    },
    {
        "trades": [("2020-01-01", 1), ("2020-02-01", -1), ("2020-03-01", 1)],
        "fifo": [("2020-03-01", 1)],
        "lifo": [("2020-03-01", 1)],
    },
    {
        "trades": [("2020-01-01", 2), ("2020-02-01", -1), ("2020-03-01", 1)],
        "fifo": [("2020-01-01", 1), ("2020-03-01", 1)],
        "lifo": [("2020-01-01", 1), ("2020-03-01", 1)],
    },
    {
        "trades": [("2020-01-01", 2), ("2020-02-01", -2), ("2020-03-01", 1), ("2020-04-01", 1)],
        "fifo": [("2020-03-01", 1), ("2020-04-01", 1)],
        "lifo": [("2020-03-01", 1), ("2020-04-01", 1)],
    },
    {
        "trades": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-03-01", 1), ("2020-04-01", -2), ("2020-05-01", 1)],
        "fifo": [("2020-03-01", 1), ("2020-05-01", 1)],
        "lifo": [("2020-01-01", 1), ("2020-05-01", 1)],
    },
    {
        "trades": [
            ("2020-01-01", 1),
            ("2020-02-01", 1),
            ("2020-03-01", 1),
            ("2020-04-01", 1),
            ("2020-05-01", -2),
            ("2020-06-01", 1),
        ],
        "fifo": [("2020-03-01", 1), ("2020-04-01", 1), ("2020-06-01", 1)],
        "lifo": [("2020-01-01", 1), ("2020-02-01", 1), ("2020-06-01", 1)],
    },
    {
        "trades": [
            ("2020-01-01", 3),
            ("2020-02-01", -2),
            ("2020-03-01", -2),
            ("2020-04-01", 4),
            ("2020-05-01", -1),
            ("2020-06-01", 1),
        ],
        "fifo": [("2020-04-01", 2), ("2020-06-01", 1)],
        "lifo": [("2020-04-01", 2), ("2020-06-01", 1)],
    },
    {
        "trades": [("2020-01-01", -1), ("2020-02-01", -1), ("2020-03-01", -1)],
        "fifo": [("2020-01-01", -1), ("2020-02-01", -1), ("2020-03-01", -1)],
        "lifo": [("2020-01-01", -1), ("2020-02-01", -1), ("2020-03-01", -1)],
    },
    {
        "trades": [("2020-01-01", -1), ("2020-02-01", -1), ("2020-03-01", 1)],
        "fifo": [("2020-02-01", -1)],
        "lifo": [("2020-01-01", -1)],
    },
    {
        "trades": [("2020-01-01", -2), ("2020-02-01", -2), ("2020-03-01", -2)],
        "fifo": [("2020-01-01", -2), ("2020-02-01", -2), ("2020-03-01", -2)],
        "lifo": [("2020-01-01", -2), ("2020-02-01", -2), ("2020-03-01", -2)],
    },
    {
        "trades": [("2020-01-01", -1), ("2020-02-01", -1), ("2020-03-01", -1), ("2020-04-01", 2), ("2020-05-01", -1)],
        "fifo": [("2020-03-01", -1), ("2020-05-01", -1)],
        "lifo": [("2020-01-01", -1), ("2020-05-01", -1)],
    },
    {
        "trades": [
            ("2020-01-01", -3),
            ("2020-02-01", 2),
            ("2020-03-01", 2),
            ("2020-04-01", -4),
            ("2020-05-01", 1),
            ("2020-06-01", -1),
        ],
        "fifo": [("2020-04-01", -2), ("2020-06-01", -1)],
        "lifo": [("2020-04-01", -2), ("2020-06-01", -1)],
    },
]


def create_amount_df(trades):
    df = pd.DataFrame([{"time": time, "amount": amount} for time, amount in trades])
    df["time"] = pd.to_datetime(df["time"])
    return df


def create_trade_df(trades):
    df = create_amount_df(trades)
    df["pos"] = df["amount"].cumsum()
    return df


def create_random_trade_df(num_rows):
    df = pd.DataFrame(
        {
            "time": [pd.to_datetime("2020-01-01") + pd.Timedelta(minutes=i) for i in range(num_rows)],
            "amount": [random.randint(-1000, 1000) for i in range(num_rows)],
        }
    )
    df["pos"] = df["amount"].cumsum()
    return df


def df_equal(df1, df2):
    return df1.reset_index(drop=True).equals(df2.reset_index(drop=True))


if __name__ == "__main__":
    for case in TEST_CASES:
        print(f"Starting case: {case}")
        trades_df = create_trade_df(case["trades"])

        result = fifo_trade_amounts(trades_df)
        expected = create_amount_df(case["fifo"])
        assert df_equal(result, expected)
        print(f"PASSED FIFO\nExpected:\n{expected}\nResult:\n{result}\n\n")

        result = lifo_trade_amounts(trades_df)
        expected = create_amount_df(case["lifo"])
        assert df_equal(result, expected)
        print(f"PASSED LIFO\nExpected:\n{expected}\nResult:\n{result}\n\n\n\n")

    print("DONE Correctness")

    # TEST_NUM_ROWS = [10, 100, 1000, 10000, 100000, 1000000]
    # for num_rows in TEST_NUM_ROWS:
    #     print(f"Creating trade df with {num_rows} rows\n")
    #     df = create_random_trade_df(num_rows)

    #     print("Running FIFO")
    #     start_time = time.time()
    #     fifo_trade_amounts(df.copy())
    #     end_time = time.time()
    #     print(f"FIFO Num rows: {num_rows} took {end_time - start_time}\n\n")
    #     print("Running LIFO")
    #     start_time = time.time()
    #     lifo_trade_amounts(df.copy())
    #     end_time = time.time()
    #     print(f"LIFO Num rows: {num_rows} took {end_time - start_time}\n\n")

    # print("DONE Performance")

    print("DONE")
