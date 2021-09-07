# coding=utf-8

"""
title:  下载数据
author：liangdongning
date:   2021/7/8
"""

from base.list_base import *
from base.data_base import *
from multiprocessing import Pool
from concurrent.futures import ThreadPoolExecutor
from base.log import *

# ===参数设定
THREAD_POOL_NUM = 20


def get_index_data(code):
    index_data = IndexData()
    index_data.request_data(code)


def get_all_index_data():
    index_list = IndexList()
    info_df = index_list.get_df()
    # 多进程版本
    # pool = Pool(THREAD_POOL_NUM)
    # for row in info_df.iterrows():
    #     code = row[1]["CODE"]
    #     pool.apply_async(func=get_index_data, args=(code,))
    # pool.close()
    # pool.join()

    # 多线程版本
    with ThreadPoolExecutor(max_workers=THREAD_POOL_NUM) as pool:
        for row in info_df.iterrows():
            code = row[1]["CODE"]
            pool.submit(get_index_data, code)


def get_stock_data(code):
    stock_data = StockData()
    stock_data.request_data(code)


def get_all_stock_data():
    stock_list = StockList()
    info_df = stock_list.get_df()
    # 多进程版本
    # pool = Pool(THREAD_POOL_NUM)
    # for row in info_df.iterrows():
    #     code = row[1]["CODE"]
    #     pool.apply_async(func=get_stock_data, args=(code,))
    # pool.close()
    # pool.join()

    # 多线程版本
    with ThreadPoolExecutor(max_workers=THREAD_POOL_NUM) as pool:
        for row in info_df.iterrows():
            code = row[1]["CODE"]
            pool.submit(get_stock_data, code)


def get_stock_list():
    stock_info = StockList()
    stock_info.request_data()


def get_index_list():
    index_info = IndexList()
    index_info.request_data()


if __name__ == "__main__":
    init_console_log()
    # get_stock_list()
    # get_index_list()
    get_all_stock_data()
    # get_all_index_data()
