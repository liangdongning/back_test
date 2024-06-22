# -*- coding: utf-8 -*-
import datetime
import os


# PathConfig 类: 存储项目相关路径配置
class PathConfig:
    current_project_dir = "F:\\软件开发项目\\back_test\\"  # 工程目录
    data_folder = os.path.join(current_project_dir, 'data\\')
    stock_folder = "F:\\stock_data\\"  # 数据目录路径
    stock_daily_folder = stock_folder + "stock-trading-data-pro\\"
    stock_daily_folder_test = stock_folder + "stock-trading-data-pro-test\\"
