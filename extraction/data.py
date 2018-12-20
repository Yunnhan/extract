import pandas as pd
from bs4 import BeautifulSoup


class Data:
    """Data数据类，实例化的时候，会通过self._load_data方法加载需要的数据，将id, title, info, cate_id, table_name等信息信息存入
       该类的属性，并可以直接通过get_info_text等方法直接获取其去掉标签后的info字段内容

       这是一个基类，不同的任务中，需要使用不同的方式加载数据。例如，kafaka任务中，需要从json格式加载数据。在bid表提取任务中，
       需要从数据库加载数据。 子类需要实现的方法是 _load_data()

        属性：
            table_name：表名
            id: 数据库id
            pattern：re正则表达式
            info: 正文包含html的内容
            title：标题
            cate_id: 所属类别， 1招标， 2中标
        方法：
            is_valid 是否为有效的数据（info字段不为空）
            get_info_text 获取去除html标签的info字段内容
            get_info_soup 获取BeautifulSoup解析后的soup对象
            get_info_sequence 获取去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
            get_tag_sequence 获取所有的p，div，span标签的BeautifulSoup对象
            get_dfs 获取文本中所有的df

    """

    def __init__(self):
        self.pattern = None  # 正则表达式模块， 位置在extraction/pattern

        self.table_name = None  # 数据库表名
        self.id = None  # 数据库表中数据id
        self.title = None  # 标题
        self.info = None  # info字段，直接从网站上爬去的包含有html的文本
        self.cate_id = None  # 类别id 1招标/2中标

        self.dfs = None  # info字段中的所有表格
        self.info_sequence = None  # info字段去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
        self.tags_sequence = None  # info字段所有的p，div，span标签的BeautifulSoup对象
        self.info_text = None  # info字段去除html标签的info字段内容
        self.info_soup = None  # info字段BeautifulSoup解析后的soup对象

        self._load_data()

    # 在不同的任务中，通过不同的方式，加载数据。例如，kafaka任务中，需要从json格式加载数据。在bid表提取任务中，需要从数据库加载数据。
    def _load_data(self):
        pass

    def is_valid(self):
        """是否为有效的数据（info字段不为None）
        :return:  True/False
        """
        if self.info is None:  # 判断info是否为None
            return False
        else:
            return True

    def get_info_text(self):
        """获取去除html标签的info字段内容
        :return:  str/None
        """
        if not self.info:  # 检查info字段是否存在内容
            return None
        if self.info_text:  # 检查是否已经有程序获取了info_text，避免反复获取。
            return self.info_text
        soup = self.get_info_soup()  # 获取BeautifulSoup解析后的soup对象
        text = soup.text  # 获取纯文本内容
        if not text:  # 剔除其内容为空字符串的情况
            return None
        else:
            self.info_text = text  # 将获取的结果置为属性， 下次再获取该信息的时候，就直接从属性获取/
            return text

    def get_info_soup(self):
        """获取BeautifulSoup解析后的soup对象
        :return: soup对象/None
        """
        if self.info_soup:  # 检查是否已经有程序获取了info_suop，避免反复获取。
            return self.info_soup
        if not self.info:  # 检查info字段是否存在内容
            return None
        soup = BeautifulSoup(self.info, 'lxml')  # 获取BeautifulSoup解析后的soup对象
        self.info_soup = soup  # 将获取的结果置为属性， 下次再获取该信息的时候，就直接从属性获取/
        return soup

    def get_tag_sequence(self):
        """ 获取所有的p，div，span标签的BeautifulSoup对象
        :return:  list of beautiful_soup's tag object
        """
        if self.tags_sequence:  # 检查是否已经有程序获取了tag_sequence，避免反复获取。
            return self.tags_sequence
        if not self.info:   # 检查info字段是否存在内容
            return None
        soup = self.get_info_soup()  # 获取BeautifulSoup解析后的soup对象
        text_segs = soup.find_all(name=['p', 'div', 'span'])  # 获取所有的p，div，span标签的BeautifulSoup对象
        if not text_segs:  # 检查是否存在p，div，span标签
            return None
        self.tags_sequence = text_segs  # 将获取的结果置为属性， 下次再获取该信息的时候，就直接从属性获取/
        return text_segs

    def get_info_sequence(self):
        """获取去除html标签后，文本分段之后的内容（通过换行符，冒号，空格等将文本分为不同的‘序列’）
        :return: 列表（列表中的每一个元素为字符串）
        """
        if self.info_sequence:  # 检查是否已经有程序获取了info_sequence，避免反复获取。
            return self.info_sequence
        if not self.info:   # 检查info字段是否存在内容
            return None
        info_text = self.get_info_text()  # 获取去除html标签的info字段内容
        if info_text is None:  # 检查info字段是否具有纯文本
            return None
        # 通过换行符，冒号，空格等将文本分为不同的‘序列’， pattern 位于 extraction/pattern
        info_sequence = self.pattern.SPLIT_PATTERN.split(info_text)
        if not info_sequence:  # 检查是否存在结果（not really need）
            return None
        self.info_sequence = info_sequence  # 将获取的结果置为属性， 下次再获取该信息的时候，就直接从属性获取/
        return info_sequence

    def get_dfs(self):
        """获取文本中所有的df, 获取info字段中所有的表格，并解析为pandas中的data frame数据结构
        :return: list of dfs
        """
        if not self.info:  # 检查info字段是否存在内容
            return None
        if self.dfs is not None:  # 检查是否已经有程序获取了df，避免反复获取。
            return self.dfs
        try:
            html_dfs = pd.read_html(self.info)  # 读取为df
        except Exception as e:  # 确保读取失败之后程序能够正常运行
            return None
        # 当存在极大的df的时候，直接返回为None（舍弃掉所有的df）， 过大的df在后续解析的时候会占用较高的内存，可能导致程序崩溃
        html_dfs = self._remove_very_big_df(html_dfs)
        # 统一df的格式，缺失值填充，变换为字符串类型，去除空白，列索引统一为列数，行索引统一为行数
        html_dfs = self._make_sure_df(html_dfs)
        if not html_dfs:  # 检查是否存在df
            return None
        self.dfs = html_dfs  # 将获取的结果置为属性， 下次再获取该信息的时候，就直接从属性获取/
        return html_dfs

    @staticmethod  # 暂时未使用
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

    def _remove_middle_big_df(self, pandas_dfs):  # 暂时未使用
        """去除行大于100， 列大于20的df
        解决3880588 这样的情况，df读取错误，df特别小，或者有点大
        :param pandas_dfs:
        :return:
        """
        if pandas_dfs is None:  # 检查输入是否为None
            return None
        if not pandas_dfs:  # 检查是否为空列表 （not really need）
            return None
        pandas_dfs = list(filter(self._is_middle_big_df, pandas_dfs))  # 去除行大于100， 列大于20的df
        return pandas_dfs

    @staticmethod  # 暂时未使用
    def _is_middle_big_df(df):
        if df.shape[0] > 100:
            return False
        if df.shape[1] > 20:
            return False
        return True

    @staticmethod
    def _remove_very_big_df(pandas_dfs):
        """当存在极大的df的时候，直接返回为None（舍弃掉所有的df）
        df的数量超过20，或者存在一个df的行数或者列数大于100， 直接舍弃掉所有的df。原因是处理极大的df时候，会占用较大的内存，
        可能会导致程序崩溃。
        :param pandas_dfs:  pandas data frame
        :return:  list of df / None
        """
        if pandas_dfs is None:  # 检查输入是否为None
            return None
        if not pandas_dfs:  # 检查是否为空列表
            return None
        if len(pandas_dfs) > 20:  # 检查读取到的df数量是否超过20
            return None
        for df in pandas_dfs:  # 遍历所有的df， 检查是否有df的列或者行数量大于100
            if df.shape[0] > 100 or df.shape[1] > 100:
                return None
        return pandas_dfs

    @staticmethod
    def _make_sure_df(dfs):
        """统一df的格式
        将缺失值转换为空字符串，原因是pandas中的某些方法例如df[col].str.contains在遇到缺失值的时候会报错
        将df每一个单元格中的内容类型转换为字符串类型。
        将df每一个单元格中的内容中的空格去除
        :param dfs: pandas data frame
        :return: 统一格式的 data frame
        """
        if dfs is None:  # 检查是否传入None
            return None
        res = []  # 统一格式后的df
        for df in dfs:
            df = df.fillna('')  # 将缺失值转换未字符串
            df = df.astype('str')  # 将每个单元格内容的变为字符串
            # 将每一个单元格中的空格去掉
            for col in df.columns:
                df[col] = df[col].str.replace(' ', '')  # 去出空格
            # 当df的列名 不等于 range(df.shape[1])的时候，重置其列名， 重置为列的个数的list
            if list(df.columns) != list(range(df.shape[1])):
                df = df.T.reset_index().T  # 保留其列名中的数据，并将其当作第一行数据
                df.columns = range(df.shape[1])  # 重置其列名（列索引）
            df.index = range(df.shape[0])  # 重置其行索引
            res.append(df)
        return res