# -*- coding: utf-8 -*-
import backtrader as bt
import pandas as pd
import numpy as np
import datetime
from base.stock_processor import SingleStockDataProcessor
from base.config import PathConfig
import quantstats
import webbrowser
import os
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
from base.log import performance_log
from multiprocessing import Pool, cpu_count
from base.fast_cerebro import FastCerebro
from base.base_stock_strategy import BaseStockStrategy, StampDutyCommissionScheme

# 设置字体
os.environ["LANG"] = "zh_CN.UTF-8"
os.environ["LC_ALL"] = "zh_CN.UTF-8"
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示


class SmallCapStockDataProcessor(SingleStockDataProcessor):
    @staticmethod
    def format_data(df):
        df = df[
            [
                "交易日期",
                "开盘价_复权",
                "收盘价_复权",
                "最高价_复权",
                "最低价_复权",
                "成交量",
                "下日_开盘涨停",
                "总市值",
                "下日_是否交易",
                "下日_是否ST",
                "下日_是否退市",
            ]
        ].copy()
        df.columns = [
            "date",
            "open",
            "close",
            "high",
            "low",
            "volume",
            "limit_up",
            "market_cap",
            "is_trade",
            "is_st",
            "is_delisting",
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
                "market_cap",
                "is_trade",
                "is_st",
                "is_delisting",
            ]
        ]


# class SmallCapData(bt.feeds.PandasData):
#     lines = (
#         "limit_up",
#         "market_cap",
#         "is_trade",
#         "is_st",
#         "is_delisting",
#     )
#     params = (
#         ("limit_up", -1),
#         ("market_cap", -1),
#         ("is_trade", -1),
#         ("is_st", -1),
#         ("is_delisting", -1),
#     )

class SmallCapData(bt.feeds.PandasDirectData):
    lines = (
        "limit_up",
        "market_cap",
        "is_trade",
        "is_st",
        "is_delisting",
    )
    params = (
        ("limit_up", 7),
        ("market_cap", 8),
        ("is_trade", 9),
        ("is_st", 10),
        ("is_delisting", 11),
    )


class SmallCapStrategy(BaseStockStrategy):
    params = (("num_stocks", 5),)  # 持有的小市值股票数量

    def __init__(self):
        super().__init__()
        self.n_periods = self.p.n_periods
        self.percentage = 0.99 / self.p.num_stocks

        performance_log.debug(
            "--------- 打印 self 策略本身的 lines ----------"
        )
        performance_log.debug(self.lines.getlinealiases())
        performance_log.debug(
            "--------- 打印 self.datas 第一个数据表格的 lines ----------"
        )
        performance_log.debug(self.datas[0].lines.getlinealiases())

    def execute_trade_logic(self):
        super().execute_trade_logic()
        self.rebalance_portfolio()

    def rebalance_portfolio(self):
        # 过滤掉涨停、停牌、nan值的数据
        datas_filtered = list(
            filter(
                lambda data: len(data)
                and not np.isnan(data.market_cap[0])
                and data.is_trade[0] == 1.0
                and data.is_st[0] == 0.0
                and data.is_delisting[0] == 0.0
                and data.limit_up[0] == 0.0,
                self.datas,
            )
        )
        # 按照市值排序
        datas_new_position = sorted(datas_filtered, key=lambda x: x.market_cap[0])[
            : self.p.num_stocks
        ]
        # 删除不在继续持有的股票，进而释放资金用于买入新的股票
        datas_position = [d for d, pos in self.getpositions().items() if pos]
        performance_log.debug(f"现有持仓个数：{len(datas_position)}")
        for data in (data for data in datas_position if data not in datas_new_position):
            # 对于每个持仓，发出清仓指令，即将目标持仓百分比设置为0
            order = self.order_target_percent(data, target=0.0)
            performance_log.debug("清仓 {} ".format(data._name))

        # 对下一期继续持有的股票，进行仓位调整
        for data in (data for data in datas_position if data in datas_new_position):
            self.order_target_percent(data, target=self.percentage)
            performance_log.debug(
                "调仓 {} 仓位 {} ".format(data._name, self.percentage)
            )
            datas_new_position.remove(data)

        # 按照等权重下单新入选股票
        for data in datas_new_position:
            performance_log.debug(
                "买入 {}仓位 {}  ".format(data._name, self.percentage)
            )
            self.order_target_percent(data, target=self.percentage)

    def notify_order(self, order):
        """
        记录交易执行情况
        """

        if order.status in [order.Submitted, order.Accepted]:
            # performance_log.debug(
            #     "Order Submitted/Accepted: id:%s  " % (order.ref)
            # )
            return
        # 已经处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                performance_log.debug(
                    "BUY EXECUTED, ref:%.0f，Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s"
                    % (
                        order.ref,  # 订单编号
                        order.executed.price,  # 成交价
                        order.executed.value,  # 成交额
                        order.executed.comm,  # 佣金
                        order.executed.size,  # 成交量
                        order.data._name,
                    )
                )  # 股票名称
            else:  # Sell
                performance_log.debug(
                    "SELL EXECUTED, ref:%.0f, Price: %.2f, Cost: %.2f, Comm %.2f, Size: %.2f, Stock: %s"
                    % (
                        order.ref,
                        order.executed.price,
                        order.executed.value,
                        order.executed.comm,
                        order.executed.size,
                        order.data._name,
                    )
                )

            # 如果指令取消/交易失败, 报告结果
        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            performance_log.debug(
                "Order Canceled/Margin/Rejected: %s" % order
            )


def process_stock(args):
    """
    处理股票数据。

    该函数接收一组参数，包括CSV文件名、数据路径、开始日期、结束日期和指数数据。
    它的目的是处理指定股票的数据，并返回处理后的股票代码和数据对象。

    :param args: 元组，包含CSV文件名、数据路径、开始日期、结束日期和指数数据。
    :return: 如果处理成功，返回股票代码和SmallCapData对象；如果数据为空，则返回None。
    """
    # 从CSV文件名中提取股票代码
    csv_file, data_path, start_date, end_date, index_data = args
    stock_code = csv_file.split(".")[0]

    # 构建股票数据文件的完整路径
    read_file_path = os.path.join(data_path, "%s.csv")

    # 初始化股票数据处理器
    processor = SmallCapStockDataProcessor(
        stock_code, read_file_path, start_date, end_date
    )

    # 处理股票数据
    stock_df = processor.process_stock_data(index_data=index_data)

    # 如果处理后的数据为空，则记录警告并返回None
    if stock_df.empty:
        performance_log.warning(
            f"No data found for {stock_code}, skipping."
        )
        return None, None

    # 创建并返回股票数据对象
    data = SmallCapData(dataname=stock_df, fromdate=start_date, todate=end_date)
    return stock_code, data


if __name__ == "__main__":
    # 构建输出文件名
    output_base = os.path.splitext(os.path.basename(__file__))[
        0
    ]  # 获取当前文件名，去掉路径和扩展名
    cache_file = os.path.join(
        PathConfig.data_folder, f"{output_base}.joblib"
    )  # 拼接路径和新文件名
    output_file = os.path.join(
        PathConfig.data_folder, f"{output_base}.html"
    )  # 拼接路径和新文件名
    # 创建Cerebro实例
    cerebro = FastCerebro()
    # 回测时间段
    start_date = datetime.datetime(2008, 1, 1)  # 回测开始时间
    end_date = datetime.datetime(2022, 12, 31)  # 回测结束时间
    # 读取指数数据
    index_data = SingleStockDataProcessor.import_index_data(
        "F:/stock_data/index_data/sh000001.csv",
        back_trader_start=start_date,
        back_trader_end=end_date,
    )
    # 加载和处理股票数据
    ## 1.获取文件夹下所有csv文件路径 由于运行速度比较慢建议同时建立一个包含少量数据的文件夹用于调试，调试成功后再用全文件夹跑结果
    data_path = PathConfig.stock_daily_folder
    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]
    csv_files = [f for f in csv_files if "bj" not in f]  # 过滤掉北交所股票
    #
    # ## 2.加载数据到Cerebro
    args_list = [(csv_file, data_path, start_date, end_date, index_data) for csv_file in csv_files]
    # 是否启用多进程，默认为True
    multiprocessing_enabled = True
    if multiprocessing_enabled:
        # 使用多进程处理
        with Pool(max(cpu_count() - 1, 1)) as pool:
            results = pool.map(process_stock, args_list)
    else:
        # 单进程处理
        results = [process_stock(args) for args in args_list]

    # 过滤并构造字典
    stock_data_dict = {result[0]: result[1] for result in results if result[0] is not None}

    performance_log.info(f"Loaded {len(stock_data_dict)} stocks.")
    # 加载数据到Cerebro
    for stock_code, data in stock_data_dict.items():
        # performance_log.debug(f"Loaded {stock_code}")
        cerebro.adddata(data, name=stock_code)  # 将数据加载到Cerebro中
    performance_log.info(f"adddata done")
    # 添加策略
    cerebro.addstrategy(SmallCapStrategy, period_type="week", n_periods=1)

    # 初始资金 100,000,00
    cerebro.broker.setcash(1000000.0)
    # 防止下单时现金不够被拒绝。只在执行时检查现金够不够。
    cerebro.broker.set_checksubmit(False)
    comm_info = StampDutyCommissionScheme(stamp_duty=0.001, commission=0.0015)
    cerebro.broker.addcommissioninfo(comm_info)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name="pyfolio")

    # 策略执行
    performance_log.info("期初总资金: %.2f" % cerebro.broker.getvalue())
    results = cerebro.run(
        maxcpus=None,
        stdstats=False,
        preload=False,
        # cache_data=True,
        # replace_cache_data=False,
        # cache_file_path=cache_file,
    )  # 用单核 CPU 做优化, 禁用观察者用以提高执行速度,不做预加载
    performance_log.info("最终资金: %.2f" % cerebro.broker.getvalue())
    stats = results[0]

    # 初始化投资组合统计分析器
    portfolio_stats = stats.analyzers.getbyname("pyfolio")
    # 从分析器中提取投资组合的回报、头寸、交易和杠杆数据
    # returns: 时间序列的每日收益率。
    # positions: 每个交易日的投资组合仓位信息。
    # transactions: 所有交易的记录，包括买卖操作。
    # gross_lev: 总杠杆，即投资组合的总市场价值除以净资本。
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    # 转换回报率指数的时区，确保时间序列的一致性
    returns.index = returns.index.tz_convert(None)
    # df_weekly_returns = returns.resample('W').agg(lambda x: (1 + x).prod() - 1).shift(-1)
    # df_yearly_returns = returns.resample('Y').agg(lambda x: (1 + x).prod() - 1).shift(-1)
    # print(df_weekly_returns)
    # quantstats
    quantstats.reports.html(returns, output=output_file, title="Stock Sentiment")
    # 打开网页
    webbrowser.open_new_tab(output_file)
