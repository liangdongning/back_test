# coding=utf-8
"""
title:  网易爬取股票和指数数据
author：liangdongning
date:   2021/7/8
"""
import requests
import datetime
from time import time
import pandas as pd
import json
from io import StringIO
import os
from base.log import *

# ===参数设定
TODAY = datetime.date.strftime(datetime.date.today(), "%Y%m%d")
STOCK_FOLDER_PATH = "F:/STOCK/StockData/"
INDEX_FOLDER_PATH = "F:/STOCK/IndexData/"
CONVERTIBLE_BOND_FOLDER_PATH = "F:/STOCK/ConvertibleBondData/"


class DataBase(object):
    _url = ""
    _params = {}
    _data = {}
    _headers = {}
    _cookies = {}
    _file_path = ""

    def detect_dir(self, file_path):
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory):
            os.makedirs(directory)

    def request_data(self, code, end=TODAY):
        # 检查路径是否存在
        self.detect_dir(self._file_path)
        self._params["code"] = code
        self._params["end"] = end
        logger.info("正在获取{}数据……".format(code))
        response = requests.get(
            url=self._url,
            params=self._params,
            headers=self._headers,
            cookies=self._cookies,
        )
        logger.debug(self._url)
        logger.info("正在处理{}数据...".format(code))
        data_df = pd.read_csv(
            StringIO(response.content.decode("gbk")), skip_blank_lines=True
        )
        # logger.debug(data_df)
        data_df = data_df.sort_values(by="日期")
        if data_df.empty:
            logger.warning("{}空数据!".format(code))
        else:
            data_df.to_csv(
                self._file_path + str(code[1:]) + ".csv", encoding="gbk", index=False
            )
            logger.info("{}数据处理完成！！".format(code))

    def get_df(self):
        return self._data_df

    def get_url(self):
        return self._url

    def get_params(self):
        return self._params

    def get_headers(self):
        return self._headers


class StockData(DataBase):
    _url = "http://quotes.money.163.com/service/chddata.html"
    _params = {
        "code": "",
        "start": "19900101",
        "end": "",
        "fields": "TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP",
    }
    _headers = {
        "Cookie": "Province=0; City=0; UM_distinctid=16c05496622f1-00e8d8cb7044e48-4c312272-15f900-16c054966245cc; _ntes_nnid=0213f9288c03916f18ed2634a6a3506d,1563456793050; vjuids=1be4f793f.16c054a41b6.0.6b5b7a77d19a78; vjlast=1563456848.1563930352.13; vinfo_n_f_l_n3=ad2a50d90e25c7dc.1.4.1563456848324.1563950911150.1563963465898; usertrack=ezq0ZV03rush6S+BCCg6Ag==; _ntes_nuid=0213f9288c03916f18ed2634a6a3506d; NNSSPID=bcf860b5427949c599552390d570c1d0; _ntes_stock_recent_plate_=%7Chy006000%3A%E6%89%B9%E5%8F%91%E9%9B%B6%E5%94%AE; _ntes_stock_recent_=0601857%7C0601326%7C0600682; _ntes_stock_recent_=0601857%7C0601326%7C0600682; _ntes_stock_recent_=0601857%7C0601326%7C0600682; ne_analysis_trace_id=1563963422398; s_n_f_l_n3=ad2a50d90e25c7dc1563963422401; _antanalysis_s_id=1563963428611; pgr_n_f_l_n3=ad2a50d90e25c7dc15639634493333113",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "quotes.money.163.com",
        "Referer": "http://quotes.money.163.com / trade / lsjysj_601857.html",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/75.0.3770.100 Safari/537.36",
    }
    _file_path = STOCK_FOLDER_PATH


class IndexData(DataBase):
    _url = "http://quotes.money.163.com/service/chddata.html"
    _headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,"
        "application/signed-exchange;v=b3",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh,en-US;q=0.9,en;q=0.8,zh-TW;q=0.7,zh-CN;q=0.6",
        "Connection": "keep-alive",
        "Host": "quotes.money.163.com",
        "Referer": "http://quotes.money.163.com/trade/lsjysj_zhishu_000003.html",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/75.0.3770.100 Safari/537.36",
    }
    _params = {
        "start": "19900101",
        "fields": "TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;VOTURNOVER;VATURNOVER ",
    }
    _file_path = INDEX_FOLDER_PATH


class ConvertibleBondData(DataBase):
    _url = ""
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
    _data = {"rp": "50", "page": "1"}
    _file_path = CONVERTIBLE_BOND_FOLDER_PATH

    def request_data(self, code, end=TODAY):
        #
        timestamp = int(time() * 1000)
        # logger.debug(timestamp)
        self._url = (
            "https://www.jisilu.cn/data/cbnew/detail_hist/%s?___jsl=LST___t=%s"
            % (code, timestamp)
        )
        logger.debug(self._url)
        self._headers["Cookie"] = (
            "kbzw__Session=0dd506ll9q8hd4kv2kah54fs64; kbz_newcookie=1; kbzw_r_uname=ldn123456689; kbzw__user_login=7Obd08_P1ebax9aX48bkkqmqp62XppmtmrCW6c3q1e3Q6dvR1Yymxdau2Zqyz62a28Ld2aaxk6OUq6utzN2e2JuplKzb2Zmcndbd3dPGpKGom6qTsJiupbaxv9Gkwtjz1ePO15CspaOYicfK4t3k4OyMxbaWkqelo7OBx8rir6mkmeStlp-BuOfj5MbHxtbE3t2ooaqZpJStl5vDqcSuwKWV1eLX3IK9xtri4qGBs8nm6OLOqKSukKaPq6mrqI-omZTM1s_a3uCRq5Supaau; Hm_lvt_164fe01b1433a19b507595a43bf58262=1624849694,1624877663,1625811052,1625811098; Hm_lpvt_164fe01b1433a19b507595a43bf58262=%s"
            % (timestamp,)
        )
        logger.debug(self._headers)
        # 检查路径是否存在
        self.detect_dir(self._file_path)
        logger.info("正在获取{}数据……".format(code))
        response = requests.post(url=self._url, data=self._data, headers=self._headers)
        data = response.content.decode("utf-8")
        data_json = json.loads(data)
        logger.info("正在处理{}数据...".format(code))

        data_df = pd.json_normalize(data_json, record_path=["rows"])
        data_df.columns = [
            i.split(".")[1] if len(i.split(".")) > 1 else i for i in data_df.columns
        ]
        del data_df["id"]
        logger.debug(data_df)

        if data_df.empty:
            logger.warning("{}空数据!".format(code))
        else:
            data_df.to_csv(
                self._file_path + str(code[1:]) + ".csv", encoding="gbk", index=False
            )
            logger.info("{}数据处理完成！！".format(code))


if __name__ == "__main__":
    init_console_log()
    # ----------------------------------------
    # test StockData
    stock_data = StockData()
    stock_data.request_data(code="1001209", end=TODAY)
    # ----------------------------------------
    # testConvertibleBondData
    # convertible_bond_data = ConvertibleBondData()
    # convertible_bond_data.request_data(code="110065", end=TODAY)
