from datetime import datetime
import backtrader as bt
import matplotlib.pyplot as plt  # 由于 Backtrader 的问题，此处要求 pip install matplotlib==3.2.2
# from base.fast_cerebro import FastCerebro
from base.fast_cerebro import FastCerebro
from base.stock_processor import SingleStockDataProcessor
from base.config import PathConfig
from base.log import performance_log

plt.rcParams["font.sans-serif"] = ["SimHei"]  # 设置画图时的中文显示
plt.rcParams["axes.unicode_minus"] = False  # 设置画图时的负号显示


# class CustomData(bt.feeds.PandasData):
#     lines = ("limit_up", "limit_down")
#     params = (("limit_up", -1), ("limit_down", -1))

class CustomData(bt.feeds.PandasDirectData):
    lines = ("limit_up", "limit_down")
    params = (("limit_up", 7), ("limit_down", 8))


class TestStrategy(bt.Strategy):
    def __init__(self):
        print("--------- 打印 self 策略本身的 lines ----------")
        print(self.lines.getlinealiases())
        print("--------- 打印 self.datas 第一个数据表格的 lines ----------")
        print(self.datas[0].lines.getlinealiases())
        # 计算第一个数据集的s收盘价的20日均线，返回一个 Data feed
        # self.sma = bt.indicators.SimpleMovingAverage(self.datas[0].close, period=20)
        # print("--------- 打印 indicators 对象的 lines ----------")
        # print(self.sma.lines.getlinealiases())
        # print("---------- 直接打印 indicators 对象的所有 lines -------------")
        # print(self.sma.lines)
        # print("---------- 直接打印 indicators 对象的第一条 lines -------------")
        # print(self.sma.lines[0])

    # def prenext(self):
    #     self.next()

    def next(self):
        performance_log.get_logger().debug(
            f"日期: {self.datas[0].datetime.date(0).isoformat()}"
        )
        performance_log.get_logger().debug(
            f"收盘价, { self.datas[0].close[0]}"
        )  # 记录收盘价
        performance_log.get_logger().debug(
            f"成交量, {self.datas[0].volume[0]}"
        )  # 记录收盘价
        performance_log.get_logger().debug(
            f"涨停, {self.datas[0].limit_up[0]}"
        )  # 记录收盘价
        # num2date() 作用是将数字形式的时间转为 date 形式


if __name__ == "__main__":
    # 设置日志级别
    # performance_log.set_logging_level("INFO")
    performance_log.get_logger().debug("开始回测")
    # 创建回测实例
    cerebro = FastCerebro()

    cerebro.addstrategy(TestStrategy)
    # 读取指数数据
    index_data = SingleStockDataProcessor.import_index_data(
        "E:/邢不行量化课程学习/代码/xbx_stock_2019-pro/xbx_stock_2019/data/选股策略/sh000001.csv",
        back_trader_start=None,
        back_trader_end=None,
    )
    # 加载和处理股票数据
    data_path = PathConfig.stock_daily_folder
    read_file_path = data_path + "%s.csv"
    processor_1 = SingleStockDataProcessor("sh600466", read_file_path)
    stock_df_1 = processor_1.process_stock_data(index_data=index_data)
    processor_2 = SingleStockDataProcessor("sz002871", read_file_path)
    stock_df_2 = processor_2.process_stock_data(index_data=index_data)

    # 加载数据
    start_date = datetime(2008, 1, 1)  # 回测开始时间
    end_date = datetime(2018, 12, 31)  # 回测结束时间
    data_1 = CustomData(
        dataname=stock_df_1, fromdate=start_date, todate=end_date
    )  # 规范化数据格式
    data_2 = CustomData(
        dataname=stock_df_2, fromdate=start_date, todate=end_date
    )  # 规范化数据格式
    # 加载数据
    cerebro.adddata(data_1, name="sh600466")
    cerebro.adddata(data_2, name="sz002871")
    result = cerebro.run(maxcpus=1, stdstats=False, preload=False, cache_data=True, replace_cache_data=False)
