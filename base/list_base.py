# coding=utf-8
"""
title:  网易爬取股票和指数列表
author：liangdongning
date:   2021/7/8
"""
import requests
import io
import json
from time import time
import pandas as pd
import os
from base.log import *


# ===参数设定
BASE_PATH = "F:/STOCK/"
STOCK_FILE_PATH = BASE_PATH + "stock_info.csv"
INDEX_FILE_PATH = BASE_PATH + "index_info.csv"
CONVERTIBLE_BOND_FILE_PATH = BASE_PATH + "convertible_bond_info.csv"


class ListBase(object):
    _url = ""
    _params = {}
    _headers = {}
    _cookies = {}
    _file_path = ""
    _data_df = None

    def detect_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def __init__(self):
        self.detect_dir(BASE_PATH)
        self.request_data()

    def request_data(self):
        logger.info(self._url)
        response = requests.get(
            url=self._url,
            params=self._params,
            headers=self._headers,
            cookies=self._cookies,
        )

        data_json = json.loads(io.StringIO(response.text).read())
        self._params["count"] = data_json["total"]
        logger.info("正在获取数据……")
        response = requests.get(
            url=self._url,
            params=self._params,
            headers=self._headers,
            cookies=self._cookies,
        )
        # logger.debug(response.content)
        data_json = json.loads(io.StringIO(response.content.decode("gbk")).read())
        logger.debug(data_json)
        data_list = data_json["list"]

        columns = list(data_list[0].keys())

        self._data_df = pd.DataFrame(data_list, columns=columns)

        self._data_df.to_csv(self._file_path, encoding="gbk", index=False)
        logger.info("数据处理完成……")

    def get_df(self):
        return self._data_df

    def get_url(self):
        return self._url

    def get_params(self):
        return self._params

    def get_headers(self):
        return self._headers


class StockList(ListBase):
    _params = {
        "host": "http://quotes.money.163.com/hs/service/diyrank.php",
        "page": "0",
        "query": "STYPE:EQA",
        "fields": "NO,SYMBOL,NAME,PRICE,PERCENT,UPDOWN,FIVE_MINUTE,OPEN,YESTCLOSE,HIGH,LOW,VOLUME,TURNOVER,HS,LB,WB,"
        "ZF,PE,MCAP,TCAP,MFSUM,MFRATIO.MFRATIO2,MFRATIO.MFRATIO10,SNAME,CODE,ANNOUNMT,UVSNEWS",
        "sort": "SYMBOL",
        "order": "asc",
        "count": "20",
        "type": "query",
    }
    _headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "quotes.money.163.com",
        "Referer": "http://quotes.money.163.com/old/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/75.0.3770.100 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    _url = _params["host"]
    _file_path = STOCK_FILE_PATH


class IndexList(ListBase):
    _params = {
        "host": "/hs/service/hsindexrank.php",
        "page": "0",
        "query": "IS_INDEX:true",
        "fields": "no,SYMBOL,NAME,PRICE,UPDOWN,PERCENT,zhenfu,VOLUME,TURNOVER,YESTCLOSE,OPEN,HIGH,LOW",
        "sort": "SYMBOL",
        "order": "asc",
        "count": "25",
        "type": "query",
        "callback": "",
        "req": "31254",
    }
    _headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "quotes.money.163.com",
        "Referer": "http://quotes.money.163.com/old/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36",
    }
    _url = "http://quotes.money.163.com/hs/service/hsindexrank.php"
    _file_path = INDEX_FILE_PATH


class ConvertibleBondList(ListBase):
    _data = {"listed": "Y", "page": "1"}
    _headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Connection": "keep-alive",
        "Content-Length": "12",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.jisilu.cn",
        "Origin": "https://www.jisilu.cn",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML,"
        " like Gecko) Chrome/87.0.4280.88 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    _url = ""
    _file_path = CONVERTIBLE_BOND_FILE_PATH

    def request_data(self):
        timestamp = int(time() * 1000)
        # logger.info(timestamp)
        # -----------------------------------------
        # 获取当前上市的数据
        self._url = (
            "https://www.jisilu.cn/data/cbnew/cb_list_new/?___jsl=LST___t=%s"
            % timestamp
        )
        logger.debug(self._url)
        logger.info("正在获取数据……")
        self._headers["Cookie"] = (
            "kbzw__Session=0dd506ll9q8hd4kv2kah54fs64; kbz_newcookie=1; kbzw_r_uname=ldn123456689; kbzw__user_login=7Obd08_P1ebax9aX48bkkqmqp62XppmtmrCW6c3q1e3Q6dvR1Yymxdau2Zqyz62a28Ld2aaxk6OUq6utzN2e2JuplKzb2Zmcndbd3dPGpKGom6qTsJiupbaxv9Gkwtjz1ePO15CspaOYicfK4t3k4OyMxbaWkqelo7OBx8rir6mkmeStlp-BuOfj5MbHxtbE3t2ooaqZpJStl5vDqcSuwKWV1eLX3IK9xtri4qGBs8nm6OLOqKSukKaPq6mrqI-omZTM1s_a3uCRq5Supaau; Hm_lvt_164fe01b1433a19b507595a43bf58262=1624849694,1624877663,1625811052,1625811098; Hm_lpvt_164fe01b1433a19b507595a43bf58262=%s"
            % (timestamp,)
        )
        response = requests.post(url=self._url, data=self._data, headers=self._headers)
        data = response.content.decode("utf-8")
        data_json = json.loads(data)
        logger.info("正在处理数据……")
        self._data_df = pd.json_normalize(data_json, record_path=["rows"])
        self._data_df.columns = [
            i.split(".")[1] if len(i.split(".")) > 1 else i
            for i in self._data_df.columns
        ]
        del self._data_df["id"]
        self._data_df = self._data_df[["bond_id", "bond_nm"]]
        logger.info(self._data_df)
        # -----------------------------------------
        # 获取退市数据
        self._url = (
            "https://www.jisilu.cn/data/cbnew/delisted/?___jsl=LST___t=%s" % timestamp
        )
        response = requests.post(url=self._url, data=self._data, headers=self._headers)
        data = response.content.decode("utf-8")
        data_json = json.loads(data)
        logger.info("正在处理退市数据……")
        data_df_delisted = pd.json_normalize(data_json, record_path=["rows"])
        data_df_delisted.columns = [
            i.split(".")[1] if len(i.split(".")) > 1 else i
            for i in data_df_delisted.columns
        ]
        data_df_delisted = data_df_delisted[["bond_id", "bond_nm"]]
        self._data_df = self._data_df.append(data_df_delisted)
        logger.info(self._data_df)

        self._data_df.to_csv(self._file_path, encoding="gbk", index=False)
        logger.info("数据处理完成")


# 测试
if __name__ == "__main__":
    init_console_log()
    # ----------------------------------------
    # test IndexList
    # list_index = IndexList()
    # ----------------------------------------
    # test StockList
    # stock_index = StockList()

    # ----------------------------------------
    # test ConvertibleBondList
    #convertible_bond_list = ConvertibleBondList()
