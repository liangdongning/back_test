# -*- coding: utf-8 -*-
import backtrader as bt
import optuna
from functools import partial
from datetime import datetime
from base.stock_processor import SingleStockDataProcessor
from base.config import PathConfig
import quantstats
import webbrowser
import os
import matplotlib.pyplot as plt
from base.log import performance_log

# 设置字体和环境变量
os.environ["LANG"] = "zh_CN.UTF-8"
os.environ["LC_ALL"] = "zh_CN.UTF-8"
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False


class CustomData(bt.feeds.PandasData):
    lines = ("limit_up", "limit_down")
    params = (("limit_up", -1), ("limit_down", -1))


class SmaStrategy(bt.Strategy):
    params = (
        ("period_short", 10),
        ("period_long", 90),
        ("printlog", False),
    )

    def __init__(self):
        """
        初始化函数
        """
        self.data_close = self.datas[0].close  # 指定价格序列

        # 初始化交易指令、买卖价格和手续费
        self.order = None
        self.buy_price = None
        self.buy_comm = None
        # 添加移动均线指标
        self.sma_short = bt.indicators.SMA(
            self.data_close, period=self.params.period_short
        )
        self.sma_long = bt.indicators.SMA(
            self.data_close, period=self.params.period_long
        )
        # help(bt.indicators.SMA)
        # print('sma1', self.sma.get(ago=0, size=3))

        # self.sma = bt.talib.SMA(self.data_close, timeperiod=self.params.period)
        self.buy_sig = self.sma_short > self.sma_long  # 短线上穿长线
        self.sell_sig = self.sma_short < self.sma_long  # 长线上穿短线

    def next(self):
        """
        主逻辑
        """
        # print(self.data.lines.getlinealiases())
        # print("日期", self.datas[0].datetime.date(0))
        # print("涨停", self.datas[0].limit_up[0])
        # print("跌停", self.datas[0].limit_down[0])
        # print('sma0-2', self.sma.get(ago=0, size=3))
        # print('sma-1', self.sma[-1])
        # print('sma-2', self.sma[-2])

        self.log(f"收盘价, {self.data_close[0]}")  # 记录收盘价
        if self.order:  # 检查是否有指令等待执行,
            return
        # 检查是否持仓
        if not self.position:  # 没有持仓
            # 执行买入条件判断：收盘价格上涨突破15日均线
            # if self.data_close[0] > self.sma[0]:
            if self.buy_sig:
                self.log("BUY CREATE, %.2f" % self.data_close[0])
                # 执行买入
                self.order = self.order_target_percent(target=0.95)
        else:
            # 执行卖出条件判断：收盘价格跌破15日均线
            # if self.data_close[0] < self.sma[0]:
            if self.sell_sig:
                self.log("SELL CREATE, %.2f" % self.data_close[0])
                # 执行卖出
                self.order = self.order_target_percent(target=0)

    def log(self, txt, dt=None, do_print=False):
        """
        Logging function fot this strategy
        """
        if self.params.printlog or do_print:
            dt = self.datas[0].datetime.date(0)
            performance_log.debug("%s, %s" % (dt.isoformat(), txt))

    def notify_order(self, order):
        """
        记录交易执行情况
        """

        if order.status in [order.Submitted, order.Accepted]:
            return
        # 已经处理的订单
        if order.status in [order.Completed, order.Canceled, order.Margin]:
            if order.isbuy():
                self.log(
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
                self.log(
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
            self.log("交易失败")
        self.order = None

    def notify_trade(self, trade):
        """
        记录交易收益情况
        """
        if not trade.isclosed:
            return
        self.log(f"策略收益：\n毛收益 {trade.pnl:.2f}, 净收益 {trade.pnlcomm:.2f}")

    def stop(self):
        """
        回测结束后输出结果
        """
        self.log(
            "(MA均线短期周期： %2d日，长周期周期： %2d日) 期末总资金 %.2f"
            % (
                self.params.period_short,
                self.params.period_long,
                self.broker.getvalue(),
            ),
            do_print=False,
        )


def objective(trial, code, stock_df, all_returns):
    cerebro = bt.Cerebro()
    # 定义参数搜索空间
    period_short = trial.suggest_int("period_short", 5, 20)
    period_long = trial.suggest_int("period_long", 20, 100)

    cerebro.addstrategy(SmaStrategy, period_short=period_short, period_long=period_long)

    # 加载数据
    start_date = datetime(2008, 1, 1)
    end_date = datetime(2018, 12, 31)
    stock_data = CustomData(dataname=stock_df, fromdate=start_date, todate=end_date)
    cerebro.adddata(stock_data, name=code)

    # 设置初始资金、佣金和滑点
    cerebro.broker.setcash(1000000.0)
    cerebro.broker.setcommission(commission=0.0003)
    cerebro.broker.set_slippage_perc(perc=0.0001)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name="pyfolio")

    cerebro.run(stdstats=False)
    # 初始化投资组合统计分析器
    portfolio_stats = cerebro.runstrats[0][0].analyzers.getbyname("pyfolio")
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    returns.index = returns.index.tz_convert(None)
    # 保存到数组当中
    all_returns.append(returns)

    final_value = cerebro.broker.getvalue()
    performance_log.info(f"period_short: {period_short} period_long: {period_long}")
    performance_log.info(f"Final value: {final_value}")
    return final_value


def main(code="sz000002"):
    output_base = os.path.splitext(os.path.basename(__file__))[0]
    all_returns = []

    # 读取股票数据
    data_path = "E:/邢不行量化课程学习/代码/xbx_stock_2019-pro/xbx_stock_2019/data/择时策略-回测/"
    read_file_path = data_path + "%s.csv"
    processor = SingleStockDataProcessor(code, read_file_path)
    stock_df = processor.process_stock_data()
    #
    # 加载数据
    # start_date = datetime(2008, 1, 1)
    # end_date = datetime(2018, 12, 31)
    # stock_data = CustomData(dataname=stock_df, fromdate=start_date, todate=end_date)

    # 创建Optuna研究对象
    study = optuna.create_study(direction="maximize")
    # 运行优化
    partial_objective = partial(objective, stock_df=stock_df, code=code, all_returns = all_returns)
    study.optimize(partial_objective, n_trials=150, n_jobs=-1)

    # 使用最优参数对应的回报序列生成报告
    best_trial_index = study.best_trial.number
    best_returns = all_returns[best_trial_index]

    # 生成量化统计报告
    output_file = os.path.join(PathConfig.data_folder, f"{output_base}_optimized.html")
    quantstats.reports.html(best_returns, output=output_file, title="Stock Sentiment")
    webbrowser.open_new_tab(output_file)


if __name__ == "__main__":
    main(code="sz000002")
