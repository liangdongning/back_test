# -*- coding: utf-8 -*-
import backtrader as bt
from base.log import performance_log
# from base.performance_timer import performance_timer


class BaseStockStrategy(bt.Strategy):
    params = (
        ("period_type", "day"),  # 'day', 'week', or 'month'
        ("n_periods", 3),  # n times of the period type
    )

    def __init__(self):
        self.day_count = 0
        timer_params = self.get_timer_params()
        self.add_timer(**timer_params)

    # 根据周期类型配置定时器参数
    def get_timer_params(self):
        timer_params = {
            "when": bt.Timer.SESSION_START,
        }

        if self.p.period_type == "week":
            timer_params.update({"weekdays": [5], "weekcarry": True})
        elif self.p.period_type == "month":
            timer_params.update({"monthdays": [1], "monthcarry": True})

        return timer_params

    # 定时器触发时的处理函数
    def notify_timer(self, timer, when, *args, **kwargs):
        if self.day_count % self.p.n_periods == 0:
            self.execute_trade_logic()
        self.day_count += 1

    # 执行交易逻辑的方法，待实现
    def execute_trade_logic(self):
        # 这里是你的具体交易逻辑
        performance_log.info("Executing trade logic")
        performance_log.debug(
            f"交易日期: {self.datas[0].datetime.date(0).isoformat()}"
        )

    # 策略的下一个交易步骤
    def next(self):
        # 正常的策略逻辑
        pass


class StampDutyCommissionScheme(bt.CommInfoBase):
    """
    本佣金模式下，买入股票仅支付佣金，卖出股票支付佣金和印花税.
    """

    params = (
        ("stamp_duty", 0.001),  # 印花税率
        ("commission", 0.00015),  # 佣金率
        ("stocklike", True),
        ("commtype", bt.CommInfoBase.COMM_PERC),
    )

    def _getcommission(self, size, price, pseudoexec):
        """
        If size is greater than 0, this indicates a long / buying of shares.
        If size is less than 0, it idicates a short / selling of shares.
        """

        if size > 0:  # 买入，不考虑印花税
            return size * price * self.p.commission
        elif size < 0:  # 卖出，考虑印花税
            return -size * price * (self.p.stamp_duty + self.p.commission)
        else:
            return 0  # just in case for some reason the size is 0.
