import pandas as pd
from extraction import pattern
from algorithm.create_df.read_data_lib.data_base import DataSQL



class DataFramePre:
    """
    将info字段转化为有效pandas的data frame数据结构.
    """

    def __init__(self, pattern):
        # 关键词，用以筛选无用的DataFrame。当DataFrame中不包含这些关键词的时候，丢弃该DataFrame。
        self.DATA_FRAME_FILTER_KEY_WORDS = pattern.DATA_FRAME_FILTER_KEY_WORDS

    def get_all_valid_pandas_df(self, data):
        """获得有效的df"""
        if not data.is_valid():
            return None
        html_dfs = data.get_dfs()
        if html_dfs is None:
            return None
        # 筛选出有效的df
        html_dfs_filter_obj = filter(lambda df: self.DATA_FRAME_FILTER_KEY_WORDS.findall(df.to_string()), html_dfs)
        html_dfs = list(html_dfs_filter_obj)
        if not html_dfs:
            return None
        return html_dfs




if __name__ == '__main__':
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    from algorithm.feature_compute.compute import Computation
    from extraction.bid_data import BidData
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    import os
    os.chdir(os.pardir)
    cnn = DataSQL()
    df_p = DataFramePre(pattern)
    test_ids = [8897, 23599, 9751767, 8703, 19599]
    for i in test_ids:
        bid_data = BidData(cnn, pattern, i, 'stang_bid_new')
        # print(bid_data.info)
        print(df_p.get_all_valid_pandas_df(bid_data))
