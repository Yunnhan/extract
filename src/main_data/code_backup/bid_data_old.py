import pandas as pd
from bs4 import BeautifulSoup


class BidData:
    """BidData数据类，实例化的时候，会从数据库中读取该id的数据，并将id, title, info, cate_id, table_name等信息信息存入
       该类的属性，并可以直接通过get_info_text等方法直接获取其去掉标签后的info字段内容

        属性：
            table_name：表名
            id: 数据库id
            pattern：re正则表达式
            info: 正文包含html的内容
            title：标题
            cate_id: 所属类别， 1招标， 2中标
        方法：
            is_valid_bid 是否为有效的中标数据(标题中包含‘中标’或者cate_id为2, 且info字段不为空)
            is_valid 是否为有效的数据（info字段不为空）
            get_info_text 获取去除html标签的info字段内容
            get_info_soup 获取BeautifulSoup解析后的soup对象
            get_info_sequence 获取去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
            get_tag_sequence 获取所有的p，div，span标签的BeautifulSoup对象
            get_dfs 获取文本中所有的df

    """

    def __init__(self, cnn, pattern, id, table_name):
        self.table_name = table_name
        self.id = id
        self.pattern = pattern
        self.dfs = None
        self.info_sequence = None
        self.tags_sequence = None
        self.info_text = None
        self.info_soup = None
        try:
            res = cnn.read_data_from_id_and_column_names(id, 'title', 'info', 'cate_id', table_name=table_name)
        except:
            raise ValueError('读取数据错误， 不能够从数据库获取数据')
        if res is None or not res:
            self.title = None
            self.info = None
            self.cate_id = None
        else:
            title, info, cate_id = res
            self.title = title
            self.info = info
            self.cate_id = cate_id

    def _is_bid(self):
        if self.title is None or not self.title:
            return False
        if '中标' in self.title or self.cate_id == 2:
            return True
        else:
            return False

    def is_valid_bid(self):
        """是否为有效的中标数据(标题中包含‘中标’或者cate_id为2, 且info字段不为空)
        :return: True/False
        """
        if self.info is None or not self.info or len(self.get_info_text()) > 50000:  # 暂时解决下4924792 这样的情况，存在一个巨大表格
            return False
        if not self._is_bid():
            return False
        return True

    def is_valid(self):
        """是否为有效的数据（info字段不为空）
        :return:  True/False
        """
        if self.info is None:
            return False
        else:
            return True

    def get_info_text(self):
        """获取去除html标签的info字段内容
        :return:  str/None
        """
        if not self.info:
            return None
        if self.info_text:
            return self.info_text
        soup = self.get_info_soup()
        text = soup.text
        if not text:
            return None
        else:
            self.info_text = text
            return text

    def get_info_soup(self):
        """获取BeautifulSoup解析后的soup对象
        :return: soup对象/None
        """
        if self.info_soup:
            return self.info_soup
        if not self.info:
            return None
        soup = BeautifulSoup(self.info, 'lxml')
        self.info_soup = soup
        return soup

    def get_tag_sequence(self):
        """ 获取所有的p，div，span标签的BeautifulSoup对象
        :return:  list of beautiful_soup's tag object
        """
        if self.tags_sequence:
            return self.tags_sequence
        if not self.info:
            return None
        soup = self.get_info_soup()
        text_segs = soup.find_all(name=['p', 'div', 'span'])
        if not text_segs:
            return None
        self.tags_sequence = text_segs
        return text_segs

    def get_info_sequence(self):
        """获取去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
        :return: 列表（列表中的每一个元素为字符串）
        """
        if self.info_sequence:
            return self.info_sequence
        if not self.info:
            return None
        info_text = self.get_info_text()
        if info_text is None:
            return None
        info_sequence = self.pattern.SPLIT_PATTERN.split(info_text)
        if not info_sequence:
            return None
        self.info_sequence = info_sequence
        return info_sequence

    def get_dfs(self):
        """获取文本中所有的df
        :return: list of dfs
        """
        if not self.info:
            return None
        if self.dfs is not None:
            return self.dfs
        try:
            html_dfs = pd.read_html(self.info)
        except Exception as e:
            # print(e)
            return None
        html_dfs = self._remove_very_big_df(html_dfs)
        # html_dfs = self._remove_middle_big_df(html_dfs)
        # html_dfs = self._remove_very_small_df(html_dfs)
        html_dfs = self._make_sure_df(html_dfs)
        if not html_dfs:
            return None
        self.dfs = html_dfs
        return html_dfs

    @staticmethod
    def _remove_very_small_df(pandas_dfs):
        """去除只有一行的df
        解决3880588 这样的情况，df读取错误，df特别小，或者有点大
        :param pandas_dfs:
        :return:
        """
        if pandas_dfs is None:
            return None
        if not pandas_dfs:
            return None
        pandas_dfs = list(filter(lambda df: df.shape[0] == 1, pandas_dfs))
        return pandas_dfs

    def _remove_middle_big_df(self, pandas_dfs):
        """去除行大于100， 列大于20的df
        解决3880588 这样的情况，df读取错误，df特别小，或者有点大
        :param pandas_dfs:
        :return:
        """
        if pandas_dfs is None:
            return None
        if not pandas_dfs:
            return None
        pandas_dfs = list(filter(self._is_middle_big_df, pandas_dfs))
        return pandas_dfs

    @staticmethod
    def _is_middle_big_df(df):
        if df.shape[0] > 100:
            return False
        if df.shape[1] > 20:
            return False
        return True

    @staticmethod
    def _remove_very_big_df(pandas_dfs):
        if pandas_dfs is None:
            return None
        if not pandas_dfs:
            return None
        if len(pandas_dfs) > 20:
            return None
        for df in pandas_dfs:
            if df.shape[0] > 100 or df.shape[1] > 100:
                return None
        return pandas_dfs

    @staticmethod
    def _make_sure_df(dfs):
        if dfs is None:
            return None
        res = []
        for df in dfs:
            df = df.fillna('')
            df = df.astype('str')
            # df.columns = df.columns.str.replace(' ', '')
            df.columns = range(df.shape[1])
            df.index = range(df.shape[0])
            res.append(df)
        return res


if __name__ == '__main__':
    import sys
    sys.path.append('..')
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    from extraction import pattern
    cnn = DataSQL()
    data = BidData(cnn, pattern, 11408442, 'stang_bid_new')
    ids = [3880588]
    for id in ids:
        data = BidData(cnn, pattern, id, 'stang_bid_new')
        print(data.info, data.get_info_sequence(), data.get_info_text(), data.get_tag_sequence(), data.is_valid())
        print(data.get_dfs()[1])

