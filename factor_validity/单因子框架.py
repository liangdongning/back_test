# -*- coding: utf-8 -*-
from base.stock_processor import SingleStockDataProcessor
import pandas as pd
import numpy as np
import os
import glob
from base.log import performance_log
from multiprocessing import Pool, cpu_count
from base.config import PathConfig
import alphalens


class SingleFactorStockDataProcessor(SingleStockDataProcessor):
    @staticmethod
    def format_data(df):
        selected_columns = [
            "交易日期",
            "股票代码",
            "开盘价_复权",
            "收盘价_复权",
            "最高价_复权",
            "最低价_复权",
            "成交量",
            "总市值",
            "新版申万一级行业名称",
        ]
        renamed_columns = [
            "date",
            "stock_code",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "factor",
            "industry",
        ]

        df = df[selected_columns].rename(
            columns=dict(zip(selected_columns, renamed_columns))
        )
        df.index = pd.to_datetime(df["date"])

        return df

    def process_stock_data(self, index_data=None):
        df = self.read_data()
        if df.empty:
            return df
        df = self.calculate_adjusted_prices(df)
        df = self.format_data(df)
        # performance_log.debug(
        #     f"Data read successfully for stock code {self.stock_code} "
        # )
        return df


class MultiStockProcessor:
    def __init__(
        self,
        data_path,
        start_date,
        end_date,
        hdf_cache_path="all_stocks_data.h5",
        use_multiprocessing=True,
    ):
        self.data_path = data_path
        self.start_date = start_date
        self.end_date = end_date
        self.hdf_cache_path = hdf_cache_path
        self.use_multiprocessing = use_multiprocessing

        # 自动检测data_path目录下的所有CSV文件，并从中提取股票代码
        self.stock_codes = [
            os.path.splitext(os.path.basename(f))[0]
            for f in glob.glob(os.path.join(data_path, "*.csv"))
        ]

    def _process_stock(self, stock_code):
        """
        处理单个股票数据的内部方法。
        """
        # 构建股票数据文件的完整路径
        read_file_path = os.path.join(self.data_path, "%s.csv")
        processor = SingleFactorStockDataProcessor(
            stock_code,
            read_file_path,
            self.start_date,
            self.end_date,
        )
        df = processor.process_stock_data()
        # 如果处理后的数据为空，则记录警告并返回None
        if df.empty:
            performance_log.warning(f"No data found for {stock_code}, skipping.")
            return None

        return stock_code, df

    def _merge_and_save_to_hdf(self, dfs):
        """
        合并多个DataFrame为一个，并保存到HDF文件中。
        """
        merged_df = pd.concat(dfs, ignore_index=True)
        merged_df.to_hdf(self.hdf_cache_path, "df", mode="w")

    def _load_merged_from_hdf(self) -> pd.DataFrame:
        """
        从HDF文件加载合并的DataFrame。
        """
        merged_df = pd.read_hdf(self.hdf_cache_path, "df")
        return merged_df

    def load_or_process_stocks(self):
        """
        根据配置选择单进程或多进程处理所有股票数据，并保存/加载到HDF文件。
        """
        if os.path.exists(self.hdf_cache_path):
            print("Loading merged data from HDF cache...")
            return self._load_merged_from_hdf()
        else:
            print("Processing all stocks...")
            if self.use_multiprocessing:
                print("Using multiprocessing...")
                with Pool(processes=max(cpu_count() - 1, 1)) as pool:
                    results = pool.map(self._process_stock, self.stock_codes)
            else:
                print("Using single processing...")
                results = [self._process_stock(args) for args in self.stock_codes]

            # 过滤并构造字典
            stock_data_dict = {
                result[0]: result[1] for result in results if result is not None
            }
            performance_log.info(f"Loaded {len(stock_data_dict)} stocks.")
            # 保存到HDF文件
            self._merge_and_save_to_hdf(stock_data_dict)
            return self._load_merged_from_hdf()


if __name__ == "__main__":
    output_base = os.path.splitext(os.path.basename(__file__))[
        0
    ]  # 获取当前文件名，去掉路径和扩展名
    output_file = os.path.join(
        PathConfig.data_folder, f"{output_base}.h5"
    )  # 拼接路径和新文件名
    processor = MultiStockProcessor(
        data_path=PathConfig.stock_daily_folder,
        start_date="2008-01-01",
        end_date="2024-01-01",
        hdf_cache_path=output_file,
        use_multiprocessing=True,
    )
    merged_df = processor.load_or_process_stocks()
    performance_log.info(merged_df.head(5))
    # 数据不是正态分布做对数处理
    merged_df["factor"] = np.log(merged_df["factor"])
    # merged_df["factor"].hist(bins=100, figsize=(18, 9))
    # plt.show()
    # print(merged_df["factor"].skew())
    # 去极值
    merged_df["factor"] = SingleFactorStockDataProcessor.winsorize_series(
        merged_df["factor"]
    )

    # 标准化
    merged_df["factor"] = SingleFactorStockDataProcessor.standardize_series(
        merged_df["factor"]
    )
    # merged_df["factor"].hist(bins=100, figsize=(18, 9))
    # plt.show()

    # 中性化
    ## 创建行业哑变量
    industry_dummies = pd.get_dummies(merged_df["industry"], prefix="industry")
    industry_dummies = industry_dummies.astype(int)
    ## 中性化处理
    merged_df["size_factor_neutralize"] = SingleFactorStockDataProcessor.neutralize(
        merged_df["factor"], industry_dummies
    )
    # merged_df[['factor', 'size_factor_neutralize']].hist(bins=100, figsize=(18, 9))
    # plt.show()

    # 数据格式转给成alphalens需要的格式
    ## 价格数据
    price_df = merged_df[["date", "stock_code", "close"]].pivot(
        index="date", columns="stock_code", values="close"
    )
    performance_log.info(price_df.head(5))
    ## 因子数据
    factor_df = merged_df[["date", "stock_code", "size_factor_neutralize"]].copy()
    factor_df = factor_df.set_index(["date", "stock_code"])  # 将时间，股票代码作为index
    performance_log.info(factor_df.head(5))
    ## 行业数据
    group_df = merged_df[["date", "stock_code", "industry"]].copy()
    group_df = group_df.sort_values(["date", "stock_code"]).set_index(
        ["date", "stock_code"]
    )
    performance_log.info(group_df.head(5))

    # alphalens数据处理
    factor_data = alphalens.utils.get_clean_factor_and_forward_returns(
        factor_df,
        price_df,
        group_df["industry"],
        quantiles=10,
        periods=(1, 5, 10),
    )
    # alphalens - -IC分析的报告完整
    alphalens.tears.create_full_tear_sheet(
        factor_data, long_short=False, group_neutral=False, by_group=False
    )
    # alphalens--IC分析的报告
    # alphalens.tears.create_information_tear_sheet(factor_data, group_neutral=False, by_group=False)
    # alphalens--收益率分析的报告
    # alphalens.tears.create_returns_tear_sheet(factor_data, long_short=False, group_neutral=False, by_group=False)
    # alphalens--换手率分析的报告
    # alphalens.tears.create_turnover_tear_sheet(factor_data)
