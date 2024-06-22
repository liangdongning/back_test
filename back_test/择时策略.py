# -*- coding: utf-8 -*-
import backtrader as bt
from datetime import datetime
from base.stock_processor import SingleStockDataProcessor
from base.config import PathConfig
import quantstats
import webbrowser
import os
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
from base.log import performance_log
# 设置字体
os.environ['LANG'] = 'zh_CN.UTF-8'
os.environ['LC_ALL'] = 'zh_CN.UTF-8'
plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示


class CustomData(bt.feeds.PandasData):
    lines = ("limit_up", "limit_down")
    params = (("limit_up", -1), ("limit_down", -1))


class SmaStrategy(bt.Strategy):
    """
    主策略程序
    """

    params = (
        ("period_short", 10),
        ("period_long", 90),
        ("printlog", True),
    )  # 全局设定交易策略的参数, period MA 均值的长度

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
            performance_log.get_logger().debug("%s, %s" % (dt.isoformat(), txt))

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


def main(code="sz000002"):
    output_base = os.path.splitext(os.path.basename(__file__))[0]  # 获取当前文件名，去掉路径和扩展名
    cerebro = bt.Cerebro()  # 创建主控制器
    cerebro.addstrategy(SmaStrategy, period_short=10, period_long=90)  # 将交易策略加载到回测系统中
    # 读取股票数据
    data_path = "E:/邢不行量化课程学习/代码/xbx_stock_2019-pro/xbx_stock_2019/data/择时策略-回测/"
    read_file_path = data_path + "%s.csv"
    # 加载和处理数据
    processor = SingleStockDataProcessor(code, read_file_path)
    stock_df = processor.process_stock_data()

    # 加载数据
    start_date = datetime(2008, 1, 1)  # 回测开始时间
    end_date = datetime(2018, 12, 31)  # 回测结束时间
    data = CustomData(
        dataname=stock_df, fromdate=start_date, todate=end_date
    )  # 规范化数据格式
    cerebro.adddata(data, name=code)  # 将数据加载至回测系统
    # 初始资金 100,000,00
    cerebro.broker.setcash(1000000.0)
    # 佣金，双边各 0.0003
    cerebro.broker.setcommission(commission=0.0003)
    # 滑点：双边各 0.0001
    cerebro.broker.set_slippage_perc(perc=0.0001)

    # 添加分析器
    cerebro.addanalyzer(bt.analyzers.PyFolio, _name='pyfolio')

    performance_log.get_logger().info("Starting Portfolio Value: %.2f" % cerebro.broker.getvalue())
    performance_log.get_logger().info("期初总资金: %.2f" % cerebro.broker.getvalue())
    results = cerebro.run(maxcpus=1, stdstats=False)  # 用单核 CPU 做优化, 禁用观察者用以提高执行速度
    performance_log.get_logger().info("最终资金: %.2f" % cerebro.broker.getvalue())
    stats = results[0]

    # 初始化投资组合统计分析器
    portfolio_stats = stats.analyzers.getbyname('pyfolio')
    # 从分析器中提取投资组合的回报、头寸、交易和杠杆数据
    # returns: 时间序列的每日收益率。
    # positions: 每个交易日的投资组合仓位信息。
    # transactions: 所有交易的记录，包括买卖操作。
    # gross_lev: 总杠杆，即投资组合的总市场价值除以净资本。
    returns, positions, transactions, gross_lev = portfolio_stats.get_pf_items()
    # 转换回报率指数的时区，确保时间序列的一致性
    returns.index = returns.index.tz_convert(None)
    # quantstats
    output_file = os.path.join(PathConfig.data_folder, f"{output_base}.html")  # 拼接路径和新文件名
    quantstats.reports.html(returns, output=output_file, title='Stock Sentiment')
    # 打开网页
    webbrowser.open_new_tab(output_file)

    # cerebro.plot()  # 画图


if __name__ == "__main__":
    main(code="sz000002")
