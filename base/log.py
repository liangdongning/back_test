# coding=utf-8
"""
title:  打印日志库
author：liangdongning
date:   2021/7/8
"""

import logging
import colorlog
from concurrent.futures import ThreadPoolExecutor

# ===参数设定
THREAD_POOL_NUMBER = 20

# 设置日志级别颜色
log_colors_config = {
    "DEBUG": "white",  # cyan white
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}
# 日志输出格式
file_formatter = logging.Formatter(
    fmt="[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s",
    datefmt="%Y-%m-%d  %H:%M:%S",
)
console_formatter = colorlog.ColoredFormatter(
    fmt="%(log_color)s[%(asctime)s.%(msecs)03d] %(filename)s -> %(funcName)s line:%(lineno)d [%(levelname)s] : %(message)s",
    datefmt="%Y-%m-%d  %H:%M:%S",
    log_colors=log_colors_config,
)
logger = logging.getLogger("logger_name")


def init_console_log():
    logger.setLevel(logging.DEBUG)
    # 输出到控制台
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(console_handler)

    console_handler.setFormatter(console_formatter)
    console_handler.close()


def init_file_log():
    # 输出到文件
    file_handler = logging.FileHandler(filename="test.log", mode="a", encoding="utf8")
    file_handler.setLevel(logging.INFO)
    if not logger.handlers:
        logger.addHandler(file_handler)

    file_handler.setFormatter(file_formatter)
    file_handler.close()


def test_log(thread_id):
    logger.info("普通打印{}".format(thread_id))
    logger.debug("debug")
    logger.info("info")
    logger.warning("warning")
    logger.error("error")
    logger.critical("critical")
    logger.info("普通打印完成{}".format(thread_id))


if __name__ == "__main__":
    init_console_log()
    #test_log()


    with ThreadPoolExecutor(max_workers=THREAD_POOL_NUMBER) as executor:
        for i in range(100):
            future = executor.submit(test_log, i)
            #print(future.result())
