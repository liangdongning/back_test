# -*- coding: utf-8 -*-
import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from base.log import performance_log


class StockDataProcessor:
    @staticmethod
    def calculate_adjusted_prices(df):
        df["涨跌幅"] = df["收盘价"] / df["前收盘价"] - 1
        df["复权因子"] = (1 + df["涨跌幅"]).cumprod()
        df["收盘价_复权"] = df["复权因子"] * (df.iloc[0]["收盘价"] / df.iloc[0]["复权因子"])
        df["开盘价_复权"] = df["开盘价"] / df["收盘价"] * df["收盘价_复权"]
        df["最高价_复权"] = df["最高价"] / df["收盘价"] * df["收盘价_复权"]
        df["最低价_复权"] = df["最低价"] / df["收盘价"] * df["收盘价_复权"]
        df.drop(["复权因子"], axis=1, inplace=True)
        return df

    @staticmethod
    def calculate_zdt_price_and_st(df):
        """
        计算股票当天的涨跌停价格。在计算涨跌停价格的时候，按照严格的四舍五入。
        包含st股，但是不包含新股
        涨跌停制度规则:
            ---2020年8月3日
            非ST股票 10%
            ST股票 5%

            ---2020年8月4日至今
            普通非ST股票 10%
            普通ST股票 5%

            科创板（sh68） 20%
            创业板（sz30） 20%
            科创板和创业板即使ST，涨跌幅限制也是20%

            北交所（bj） 30%

        :param df: 必须得是日线数据。必须包含的字段：前收盘价，开盘价，最高价，最低价
        :return:
        """
        if df.empty:
            return df
        # 计算涨停价格
        # 普通股票
        cond = df['股票名称'].str.contains('ST')
        df['涨停价'] = df['前收盘价'] * 1.1
        df['跌停价'] = df['前收盘价'] * 0.9
        df.loc[cond, '涨停价'] = df['前收盘价'] * 1.05
        df.loc[cond, '跌停价'] = df['前收盘价'] * 0.95

        # 2020年8月3日之后涨跌停规则有所改变
        # 新规的科创板
        new_rule_kcb = (df['交易日期'] > pd.to_datetime('2020-08-03')) & df['股票代码'].str.contains('sh68')
        # 新规的创业板
        new_rule_cyb = (df['交易日期'] > pd.to_datetime('2020-08-03')) & df['股票代码'].str.contains('sz30')
        # 北交所条件
        cond_bj = df['股票代码'].str.contains('bj')

        # 科创板 & 创业板
        df.loc[new_rule_kcb | new_rule_cyb, '涨停价'] = df['前收盘价'] * 1.2
        df.loc[new_rule_kcb | new_rule_cyb, '跌停价'] = df['前收盘价'] * 0.8

        # 北交所
        df.loc[cond_bj, '涨停价'] = df['前收盘价'] * 1.3
        df.loc[cond_bj, '跌停价'] = df['前收盘价'] * 0.7

        # 四舍五入
        df['涨停价'] = df['涨停价'].apply(
            lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))
        df['跌停价'] = df['跌停价'].apply(
            lambda x: float(Decimal(x * 100).quantize(Decimal('1'), rounding=ROUND_HALF_UP) / 100))

        # 判断是否一字涨停
        df['一字涨停'] = False
        df.loc[df['最低价'] >= df['涨停价'], '一字涨停'] = True
        # 判断是否一字跌停
        df['一字跌停'] = False
        df.loc[df['最高价'] <= df['跌停价'], '一字跌停'] = True
        # 判断是否开盘涨停
        df['开盘涨停'] = False
        df.loc[df['开盘价'] >= df['涨停价'], '开盘涨停'] = True
        # 判断是否开盘跌停
        df['开盘跌停'] = False
        df.loc[df['开盘价'] <= df['跌停价'], '开盘跌停'] = True

        # =计算下个交易的相关情况
        df['下日_一字涨停'] = df['一字涨停'].shift(-1)
        df['下日_开盘涨停'] = df['开盘涨停'].shift(-1)
        df['下日_是否ST'] = df['股票名称'].str.contains('ST').shift(-1)
        df['下日_是否退市'] = df['股票名称'].str.contains('退').shift(-1)

        return df

    @staticmethod
    def merge_with_index_data(df, index_data):
        """
        原始股票数据在不交易的时候没有数据。
        将原始股票数据和指数数据合并，可以补全原始股票数据没有交易的日期。
        :param df: 股票数据
        :param index_data: 指数数据
        :return:
        """

        # ===将股票数据和上证指数合并，结果已经排序
        df = pd.merge(
            left=df, right=index_data, on="交易日期", how="right", sort=True, indicator=True
        )

        # ===对开、高、收、低、前收盘价价格进行补全处理
        # 用前一天的收盘价，补全收盘价的空值
        df["收盘价"].fillna(method="ffill", inplace=True)
        # 用收盘价补全开盘价、最高价、最低价的空值
        df["开盘价"].fillna(value=df["收盘价"], inplace=True)
        df["最高价"].fillna(value=df["收盘价"], inplace=True)
        df["最低价"].fillna(value=df["收盘价"], inplace=True)
        # 补全前收盘价
        df["前收盘价"].fillna(value=df["收盘价"].shift(), inplace=True)

        # ===将停盘时间的某些列，数据填补为0
        fill_0_list = ["成交量", "成交额"]
        df.loc[:, fill_0_list] = df[fill_0_list].fillna(value=0)

        # ===用前一天的数据，补全其余空值
        df.fillna(method="bfill", inplace=True)
        df.fillna(method="ffill", inplace=True)

        # ===判断计算当天是否交易
        df["是否交易"] = True
        df.loc[df["_merge"] == "right_only", "是否交易"] = False
        del df["_merge"]

        df['下日_是否交易'] = df['是否交易'].shift(-1)

        df.reset_index(drop=True, inplace=True)

        return df


class SingleStockDataProcessor(StockDataProcessor):
    def __init__(self, stock_code, file_path, start_date=None, end_date=None):
        self.stock_code = stock_code
        self.file_path = file_path
        self.start_date = start_date
        self.end_date = end_date

    def read_data(self):
        try:
            df = pd.read_csv(
                self.file_path % self.stock_code,
                encoding="gbk",
                skiprows=1,
                parse_dates=["交易日期"],
            )
            df.sort_values(by=["交易日期"], inplace=True)
            df.drop_duplicates(subset=["交易日期"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            if self.start_date is not None:
                df = df[df['交易日期'] >= pd.to_datetime(self.start_date)]
            if self.end_date is not None:
                df = df[df['交易日期'] <= pd.to_datetime(self.end_date)]

            # performance_log.get_logger().debug(
            #     f"Data read successfully for stock code {self.stock_code}"
            # )
            return df
        except Exception as e:
            performance_log.get_logger().error(
                f"Error reading data for stock code {self.stock_code}: {e}"
            )
            raise

    @staticmethod
    def import_index_data(path, back_trader_start=None, back_trader_end=None):
        """
        从指定位置读入指数数据。指数数据来自于：program/构建自己的股票数据库/案例_获取股票最近日K线数据.py
        :param back_trader_end: 回测结束时间
        :param back_trader_start: 回测开始时间
        :param path:
        :return:
        """
        # 导入指数数据
        df_index = pd.read_csv(path, parse_dates=['candle_end_time'], encoding='gbk')
        df_index['指数涨跌幅'] = df_index['close'].pct_change()
        df_index = df_index[['candle_end_time', '指数涨跌幅']]
        df_index.dropna(subset=['指数涨跌幅'], inplace=True)
        df_index.rename(columns={'candle_end_time': '交易日期'}, inplace=True)

        if back_trader_start:
            df_index = df_index[df_index['交易日期'] >= pd.to_datetime(back_trader_start)]
        if back_trader_end:
            df_index = df_index[df_index['交易日期'] <= pd.to_datetime(back_trader_end)]

        df_index.sort_values(by=['交易日期'], inplace=True)
        df_index.reset_index(inplace=True, drop=True)

        return df_index

    def process_stock_data(self, index_data=None):
        df = self.read_data()
        if df.empty:
            return df
        df = self.calculate_zdt_price_and_st(df)
        df = self.calculate_adjusted_prices(df)
        if index_data is not None:
            df = self.merge_with_index_data(df, index_data)
        df = self.format_data(df)
        # performance_log.get_logger().debug(
        #     f"Data read successfully for stock code {self.stock_code} "
        # )
        return df

    # @staticmethod
    # def save_data(df, file_name):
    #     df.to_hdf(file_name, key="df", mode="w")

    @staticmethod
    def format_data(df):
        df = df[
            ["交易日期", "开盘价_复权", "收盘价_复权", "最高价_复权", "最低价_复权", "成交量", "开盘涨停", "开盘跌停"]
        ].copy()
        df.columns = [
            "date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "limit_up",
            "limit_down",
        ]
        df.index = pd.to_datetime(df["date"])
        df.loc[:, "openinterest"] = 0
        return df[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "openinterest",
                "limit_up",
                "limit_down",
            ]
        ]
