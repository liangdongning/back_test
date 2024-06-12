# -*- coding: utf-8 -*-
import logging
import os
import time
from multiprocessing import Process
from colorama import Fore, Style, init
from concurrent_log_handler import ConcurrentRotatingFileHandler
from qmttrader.base.config import PathConfig
from qmttrader.base.singleton import singleton

# 初始化 Colorama
init(autoreset=True)


# 定义一个彩色日志格式化器，根据日志级别为日志消息添加颜色
class ColoredFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": Fore.CYAN,
        "INFO": Fore.BLUE,
        "WARNING": Fore.YELLOW,
        "ERROR": Fore.RED,
        "CRITICAL": Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        log_fmt = self.COLORS.get(record.levelname, "") + self._fmt + Style.RESET_ALL
        formatter = logging.Formatter(log_fmt, datefmt="%Y-%m-%d %H:%M:%S")
        return formatter.format(record)


class ProcessSafeColoredLogger:
    def __init__(self, name, log_file="app.log", log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(log_level)

        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(
            ColoredFormatter(ProcessSafeColoredLogger.create_format())
        )
        self.logger.addHandler(console_handler)

        if log_file:
            self.create_folder_if_not_exists(self.get_directory_from_path(log_file))
            file_handler = ConcurrentRotatingFileHandler(
                log_file, maxBytes=1048576, backupCount=10
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(
                logging.Formatter(
                    ProcessSafeColoredLogger.create_format(),
                    datefmt="%Y-%m-%d %H:%M:%S",
                )
            )
            self.logger.addHandler(file_handler)

    def __del__(self):
        self.logger.handlers = []

    def get_logger(self):
        return self.logger

    def set_logging_level(self, level):
        """
        设置日志级别，同时更新控制台和文件处理器的日志级别。

        :param level: 日志级别字符串，如 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'。
        """
        try:
            self.logger.setLevel(level)

            # 更新关联的处理器级别
            for handler in self.logger.handlers:
                handler.setLevel(level)

            print(f"日志级别已设置为 {level}")
        except AttributeError:
            print(
                f"无效的日志级别: {level}。请使用 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL' 中的一个。"
            )

    @staticmethod
    def create_format():
        timestamp_format = "[%(asctime)s.%(msecs)03d]"
        file_info_format = " - [%(filename)s - %(lineno)d]"
        process_info_format = " - [%(processName)s - %(process)d]"
        thread_info_format = " - [%(threadName)s - %(thread)d]"
        level_info_format = " - %(levelname)s:"
        message_format = " %(message)s"

        console_format = (
            timestamp_format
            + file_info_format
            + process_info_format
            + thread_info_format
            + level_info_format
            + message_format
        )
        return console_format

    @staticmethod
    def create_folder_if_not_exists(folder_path):
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

    @staticmethod
    def get_directory_from_path(file_path):
        directory = os.path.dirname(file_path)
        if not directory:
            return "./"
        return directory


@singleton
class TraderLogger(ProcessSafeColoredLogger):
    def __init__(self):
        super().__init__("TraderLogger", PathConfig.trader_log_file, logging.INFO)


@singleton
class PerformanceLogger(ProcessSafeColoredLogger):
    def __init__(self):
        super().__init__("PerformanceLogger", "", logging.DEBUG)


trader_log = TraderLogger()
performance_log = PerformanceLogger()


# 定义一个任务函数，用于模拟交易和性能日志记录
# 定义一个函数，用于在多进程中执行任务并记录日志
def task(process_id):
    # 初始化日志记录器
    # trader_log = TraderLogger()
    # performance_log = PerformanceLogger()
    trader_logger = trader_log.get_logger()
    performance_logger = performance_log.get_logger()

    for i in range(2):
        trader_logger.info(f"Process  {process_id} - Trader Log: iteration {i}")
        performance_logger.debug(
            f"Process {process_id} - Performance Log: iteration {i}"
        )
        trader_logger.warning(f"Process  {process_id} - Trader Log: iteration {i}")
        trader_logger.error(f"Process  {process_id} - Trader Log: iteration {i}")
        time.sleep(1)


# 主程序入口
if __name__ == "__main__":
    user_input = input(
        "请输入新的日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）或输入 'exit' 退出："
    )
    if user_input.lower() == "exit":
        exit()
    try:
        level = getattr(logging, user_input.upper())
        trader_log.set_logging_level(level)
        performance_log.set_logging_level(level)
    except AttributeError:
        print(
            f"无效的日志级别: {user_input}。请使用 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL' 中的一个或输入 'exit' 退出。"
        )
    # 创建多个进程执行任务
    processes = []
    for i in range(1):
        process_name = f"Process aaa {i}"
        p = Process(target=task, args=(i,), name=process_name)
        processes.append(p)
        p.start()

    # 等待所有进程完成
    for p in processes:
        p.join()

    performance_log.get_logger().info("All initial processes have finished.")
    performance_log.get_logger().warning("All initial processes have finished.")
    performance_log.get_logger().error("All initial processes have finished.")

