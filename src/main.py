import pandas as pd
import sys
import os
import re
# 将algorithm\create_df路径加入到模块与包的搜索路径之中
# sys.path.append(os.path.join(os.getcwd(), 'algorithm\create_df'))
# from read_data_lib.data_base import DataSQL
from algorithm.create_df.data_frame_main import DataFramePre
from algorithm.feature_compute.compute import Computation
from algorithm.create_df.read_data_lib.data_base import DataSQL
from algorithm.nlp_algorithm.ltp_algorithm import Ner
from algorithm.bid_information import Information
from extraction.bid_data import BidData
from extraction import pattern

class Res:
    """ 测试每一个id， 提取的结果

    """
    def __init__(self):
        # 初始化Information类，Information类为主要的提取程序，通过其方法get_information获取各个字段的内容
        # 位于algorithm.bid_information
        self.ifm = Information(pattern)
        # 连接数据库
        self.cnn = DataSQL()

    def main(self, id, table_name):
        # BidData数据类，实例化的时候，会从数据库中读取该id的数据，并将id, title, info, cate_id, table_name等信息信息存入
        # 该类的属性，并可以直接通过get_info_text等方法直接获取其去掉标签后的info字段内容
        data = BidData(self.cnn, pattern, id, table_name)
        # Information类为主要的提取程序，通过其方法get_information获取各个字段的内容， 传入的是BidData对象
        return self.ifm.get_information(data)


if __name__ == '__main__':
    # 实例化
    df_pre = Res()
    # 测试id， 未用
    test_ids = [10002714, 10000726, 9751767, 10005934, 19599, 10005934, 19599, 9751767, 8703, 19599, 10000836, 10005934, 10000236,
                ]
    # 测试id
    new_ids = [100782, 10423684, 11043551, 12414575, 12242437, 12191163, 12414575, 12378640, 74782, 8783252, 10104898, 11254583, 11351661, 10126764, 11988509, 4924951, 4924792, 4924951, 4924960, 3880588, 3882099, 11835976, 10135746, 11513530, 10031940, 11656137, 10105547, 10125613, 1010414, 11408442,
               8649348, 9979233, 9493171, 10125682, 10125613, 10170842, 10195945,
               10170674, 10134433, 8435, 15266, 9415, 10105444, 10104664,
               10105547, 10105283, 10103994, 10104689, 10103847, 10001072, 10002498, 10000081, 10104377,
               10104275, 10002694, 1010414,
               10104149, 10002714, 10001270, 10000877, 10002694, 10000726, 10001172]
    # 遍历每一个测试id， 获得其各个字段的内容
    for i in new_ids:
        res = df_pre.main(i, 'stang_bid_new')
        print(res, i)
