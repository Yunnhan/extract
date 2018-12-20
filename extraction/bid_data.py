from extraction.data import Data


class BidData(Data):
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
        self.cnn = cnn  # 处理数据库的实例，包含了一些简易的读取数据和插入数据的方法
        self.pattern = pattern  # 正则表达式模块， 位置在extraction/pattern

        self.table_name = table_name  # 数据库表名
        self.id = id  # 数据库表中数据id
        self.title = None  # 标题
        self.info = None  # info字段，直接从网站上爬去的包含有html的文本
        self.cate_id = None  # 类别id 1招标/2中标

        self.dfs = None  # info字段中的所有表格
        self.info_sequence = None  # info字段去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
        self.tags_sequence = None  # info字段所有的p，div，span标签的BeautifulSoup对象
        self.info_text = None  # info字段去除html标签的info字段内容
        self.info_soup = None  # info字段BeautifulSoup解析后的soup对象

        self._load_data()

    def _load_data(self):
        # 读取数据
        res = ''
        try:
            # 读取标题，正文，以及类别信息
            res = self.cnn.read_data_from_id_and_column_names(self.id, 'title', 'info', 'cate_id', table_name=self.table_name)
        except:
            try:
                # 预留的读取批复表的语句， 批复表没有cate_id字段
                res = self.cnn.read_data_from_id_and_column_names(self.id, 'title', 'info', table_name=self.table_name)
            except:
                pass
        if res:
            self.title = res[0]  # 将标题存入属性
            self.info = res[1]  # 将info存入属性
        # 如果表中拥有cate_id字段，那么存入cate_id, 如果没有那么保持为None
        if res and len(res) == 3:
            self.cate_id = res[2]  # 将cate_id存入属性

    def _is_bid(self):
        """判断是否中标数据， cate_id == 2 或者 标题中有‘中标’关键字
        :return: True / False
        """
        if self.title is None or not self.title:  # 检查是否存在标题
            return False
        if '中标' in self.title or self.cate_id == 2:  # 检查cate_id和关键词
            return True
        else:
            return False

    def is_valid_bid(self):
        """是否为有效的中标数据(标题中包含‘中标’或者cate_id为2, 且info字段不为空)
        :return: True/False
        """
        # 暂时解决下4924792 这样的情况，存在一个巨大表格
        # 检查info字段是否存在，和 info字段中的文本是否过多
        if self.info is None or not self.info or (self.get_info_text() and len(self.get_info_text()) > 50000):
            return False
        if not self._is_bid():  # 是否为中标数据
            return False
        return True


if __name__ == '__main__':
    import sys

    sys.path.append('..')
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    from extraction import pattern

    cnn = DataSQL()
    data = BidData(cnn, pattern, 11408442, 'stang_bid_new')
    ids = [3880588, 9013364]
    for id in ids:
        data = BidData(cnn, pattern, id, 'stang_bid_new')
        print(data.info, data.get_info_sequence(), data.get_info_text(), data.get_tag_sequence(), data.is_valid())
        print(data.get_dfs()[1])

