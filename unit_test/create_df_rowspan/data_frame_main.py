import pandas as pd
import sys
import os
import re
# 将algorithm\create_df路径加入到模块与包的搜索路径之中
sys.path.append(os.path.join(os.getcwd(), 'algorithm\create_df_other1'))
from read_data_lib.data_base import DataSQL
from read_html import HtmlTables

class data_frame_pre:
    """
    将info字段转化为有效pandas的data frame数据结构.
    """

    def __init__(self):
        # 连接数据库
        self.DB = DataSQL()
        # 关键词，用以筛选无用的DataFrame。当DataFrame中不包含这些关键词的时候，丢弃该DataFrame。
        self.DATA_FRAME_FILTER_KEY_WORDS = re.compile(r'(?:中标|价|项目)')

    def _load_data(self, id, table_name):
        """读取数据"""
        id, title, info = self.DB.read_data_from_id(id, table_name)
        return id, title, info

    def _get_all_pandas_df(self, id, table_name):
        """获得有效的df"""
        id, title, info = self._load_data(id, table_name)
        if not info:
            return None
        html_dfs = HtmlTables(info).read()
        # 筛选出有效的df
        html_dfs_filter_obj = filter(lambda df: self.DATA_FRAME_FILTER_KEY_WORDS.findall(df.to_string()), html_dfs)
        html_dfs = list(html_dfs_filter_obj)
        if not html_dfs:
            return None
        return html_dfs

    def main(self, id, table_name):
        html_dfs = self._get_all_pandas_df(id, table_name)
        if html_dfs:
            debug_code = html_dfs[0]
        return html_dfs


if __name__ == '__main__':
    df_p = data_frame_pre()
    test_ids = [9751767, 8703, 19599]
    for i in test_ids:
        print(df_p.main(i, 'stang_bid_new'))
