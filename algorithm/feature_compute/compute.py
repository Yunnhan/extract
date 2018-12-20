import numpy as np
import pandas as pd
import re
import warnings
from extraction import pattern


class Computation(object):

    def __init__(self, df_type_key_pattern):
        warnings.filterwarnings("ignore", 'This pattern has match groups')  # 用于忽略pf[col].str.contain方法的警示信息
        self.KEY_PATTERN = df_type_key_pattern  # 这些关键词用以确定该表格是纵向还是横向排列的表格

    # 获取更加‘精确’还是更加‘广泛’的关键词
    @staticmethod
    def get_key_pattern(info, less_key_pattern, more_key_pattern, bad_key_pattern):
        """选择关键词pattern. 当正文中出现了bad_key_pattern的时候，使用less_key_pattern, 否者使用more_key_pattern
        (str, re.pattern, re.pattern, re.pattern) -> re.pattern
        例如： 在第一中标单位的提取过程中
        :param info: 文本的正文内容
        :param less_key_pattern:  出现bad_key_pattern使用的pattern
        :param more_key_pattern:  没有出现bad_key_pattern使用的pattern
        :param bad_key_pattern:  用以判断使用哪一类关键词的pattern
        :return: 使用的关键词pattern
        """
        if info is None:  # 检查文本是否为None
            return less_key_pattern
        if bad_key_pattern.findall(pattern.SPACE_PATTERN.sub('', info)):  # 判断文本中是否出现干扰关键词（去除空格后判断）
            return less_key_pattern  # 使用更加‘精确’的关键词
        else:
            return more_key_pattern  # 使用更加‘广泛’的关键词

    @staticmethod
    def get_distance(loc_a, loc_b):
        """获取两个位置的距离， 横向距离加上纵向距离(iter, iter) -> int
        :param loc_a: 可迭代的数据结构， list/tuple等
        :param loc_b: 可迭代的数据结构， list/tuple等
        :return:  距离
        >>> from extraction import pattern
        >>> c = Computation(pattern.KEY_PATTERN)
        >>> c.get_distance([0,1], [1,0])
        2
        >>> c.get_distance([1,1], [2, 2])
        2
        """
        if not loc_a or not loc_b:  # 检查传入的位置信息是否存在None
            return None
        try:
            loc_a = [int(ele) for ele in loc_a]  # 确保为int类型
            loc_b = [int(ele) for ele in loc_b]  # 确保为int类型
        except ValueError:
            return None
        loc_a = np.array(loc_a)  # 转换为numpy的ndarray数据结构
        loc_b = np.array(loc_b)  # 转换为numpy的ndarray数据结构
        dst = np.sum(np.abs(loc_a - loc_b))  # 获取横向距离 + 纵向距离
        return dst

    def get_distance_data(self, loc_a, loc_b):
        """获得距离信息 （iter, iter） -> (int, iter, iter)
        :param loc_a: 可迭代的数据结构， list/tuple等
        :param loc_b: 可迭代的数据结构， list/tuple等
        :return:  距离， a位置， b位置。 返回距离的同时返回a位置和b位置是为了方便后续的排序
        >>> from extraction import pattern
        >>> c = Computation(pattern.KEY_PATTERN)
        >>> c.get_distance_data([1,3], [3, 4])
        (3, [1, 3], [3, 4])
        """
        return self.get_distance(loc_a, loc_b), loc_a, loc_b

    @staticmethod
    def get_location(df, key_pattern):
        """获取单元格所在位置的索引
        使用时候需要判断返回的是
        :param df:
        :param key_pattern:
        :return:  None / data frame.

        >>> import os
        >>> os.chdir(os.pardir)
        >>> os.chdir(os.pardir)
        >>> from extraction import pattern
        >>> d = [('第一中标', 'AA公司'), ('中标', 'XXX公司'), ('第三中xx', 'A公司'), ]
        >>> df = pd.DataFrame(data=d)
        >>> dst = Computation(pattern.KEY_PATTERN)
        >>> re_p = re.compile('第[一二三]中')
        >>> # 确保列、行为数字索引:建议之前确保df的行和列都为数字索引
        >>> high, wide = df.shape
        >>> df.columns = range(wide)
        >>> df.index = range(high)
        >>> # 获得关键词的位置
        >>> locs = dst.get_location(df, re_p)
        >>> locs.loc[1, '行号']
        2
        """
        # 位置索引
        res = []
        for col in df:  # 遍历每一列
            row_index = df.ix[df[col].str.contains(key_pattern), col].index  # 获取该列中，包含关键词的索引
            if row_index.shape[0]:  # 检查是否存在行索引
                [res.append((ele, col)) for ele in row_index]  # 将该列出现关键词的位置，存入res
        if not res:  # 检查res是否为空列表
            return None
        if res:
            res_df = pd.DataFrame(res, columns=['行号', '列号'])  # 构建data frame存储关键词位置
            return res_df

    def _get_df_type(self, key_locations, is_weak):
        """获得df的查找方向，是横向查找还是纵向查找
        同一行:关键词的列号序号呈现+1增长，那么为纵向查找。
        同一列:关键词的列号行号呈现+1增长，那么为横向查找。
        :param key_locations:
        :param is_weak:
        :return:
        """
        if key_locations is None:  # 检查输入的关键词位置是否为None
            return None
        if key_locations.shape[0] == 0:  # 检查是否为空df
            return None
        loc_df = pd.DataFrame(key_locations, columns=['行号', '列号'])  # 确保关键词位置是一个df
        # 相同的一行，如果关键词呈现按照列号递增的情况。那么应该纵向查找 ex： 第一中标人 第二中标人 第三中标人
        for row, frame in loc_df.groupby('行号'):
            if self._is_increasing(frame['列号'], is_weak):  # 列号增加
                return '纵向查找'
        # 相同的一列，如果关键词呈现按照行号递增的情况。那么应该横向查找
        # ex:  第一中标人
        #      第二中标人
        #      第三中标人
        for col, frame in loc_df.groupby('列号'):
            if self._is_increasing(frame['行号'], is_weak):  # 行号增加
                return '横向查找'
        return None

    def _get_df_type_pro(self, key_locations, is_weak):
        """获取df的查找方向，是横向查找还是纵向查找。
        同一行:关键词的列号序号呈现+1增长，那么为纵向查找。
        不同列：关键词的行号呈现+1增长，那么为横向查找。
        :param key_locations:  关键词位置
        :param is_weak:  是否使用一种更加‘广泛’的判定方式
        :return: ‘横向查找’/‘纵向查找’/None
        """
        if key_locations is None:  # 检查输入的关键词位置是否为None
            return None
        if key_locations.shape[0] == 0:  # 检查是否为空df
            return None
        loc_df = pd.DataFrame(key_locations, columns=['行号', '列号'])  # 确保关键词位置是一个df
        # 相同的一行，如果关键词呈现按照列号递增的情况。那么应该纵向查找 ex： 第一中标人 第二中标人 第三中标人
        for row, frame in loc_df.groupby('行号'):
            if self._is_increasing(frame['列号'], is_weak):  # 列号增加
                return '纵向查找'
        if self._is_increasing(loc_df['行号'], is_weak):  # 行号增加
            return '横向查找'
        return None

    def get_df_type(self, df, is_weak):
        """获得df的查找方向，是横向查找还是纵向查找"""
        locs = self.get_location(df, self.KEY_PATTERN)
        return self._get_df_type(locs, is_weak)

    def get_df_type_pro(self, df, is_weak):
        """获得df的查找方向，是横向查找还是纵向查找
        同一行:关键词的列号序号呈现+1增长，那么为纵向查找。
        不同列：关键词的行号呈现+1增长，那么为横向查找。
        :param df: pandas data frame
        :param is_weak: 是否使用一种更加‘广泛’的判定方式
        :return: ‘横向查找’/‘纵向查找’/None
        """
        locs = self.get_location(df, self.KEY_PATTERN)  # 获取用来判定df类型的关键词位置
        return self._get_df_type_pro(locs, is_weak)

    @staticmethod
    def get_next_horizontal_cell(locs, df_shape=None):
        """获取下一个横向位置， 注意：获得的位置不会超出data frame -> None/df
        :param locs: pandas data frame column=['行号', '列号']
        :return: data frame
        """
        if locs is None:  # 检查
            return None
        if not isinstance(locs, pd.DataFrame) or not isinstance(locs, np.ndarray):  # 检查
            try:
                locs = pd.DataFrame(locs, columns=['行号', '列号'])  # 确保位置数据为df
            # todo 异常捕获，以及存储
            except:
                pass
        # data frame 是可变类型, 会在原来变量的位置上修改，这里copy处理，不需要inplace操作
        locs = locs.copy()
        locs['列号'] = locs['列号'] + 1  # 下一个‘横向’位置
        if df_shape:
            locs = locs[locs['列号'] < df_shape[1]]  # 去除超出df大小的位置
        return locs

    @staticmethod
    def get_next_vertical_cell(locs, df_shape=None):
        """获取下一个纵向位置， 注意：获得的位置不会超出data frame -> None/df
        :param locs: pandas data frame column=['行号', '列号']
        :return: data frame
        """
        if locs is None:  # 检查
            return None
        if not isinstance(locs, pd.DataFrame) or not isinstance(locs, np.ndarray):  # 检查
            try:
                locs = pd.DataFrame(locs, columns=['行号', '列号'])  # 确保位置数据为df
            # todo 异常捕获，以及存储
            except:
                pass
        # data frame 是可变类型, 会在原来变量的位置上修改，这里copy处理，不需要inplace操作
        locs = locs.copy()
        locs['行号'] = locs['行号'] + 1  # 下一个‘纵向’位置
        if df_shape:
            locs = locs[locs['行号'] < df_shape[0]]  # 去除超出df大小的位置
        return locs

    @staticmethod
    def get_cell_from_locs(df, locs_df):
        """ 从位置df获取， 需要的cell内容 -> None/list
        :param df: 原始df
        :param locs_df: 原始df中的位置信息（类型为df）
        :return: 原始df中，这些位置的内容
        """
        if locs_df is None:  # 检查
            return None
        if not isinstance(df, pd.DataFrame):  # 检查
            raise ValueError('参数不是Data frame')
        if not isinstance(locs_df, pd.DataFrame):  # 确保位置数据为pandas 的df
            try:
                if not isinstance(locs_df, list) and not isinstance(locs_df, tuple):
                    return None
                locs_df = pd.DataFrame(list((tuple(locs_df), )), columns=['行号', '列号'])  # 将其他类型的数据结构转换为df
            except:
                raise ValueError('位置参数不正确')
        if locs_df.shape[0] == 0:  # 检查空df
            return None
        res = []  # 获取的结果
        for index, row in locs_df.iterrows():  # 遍历s'y
            try:
                res.append(df.iloc[row['行号'], row['列号']])
            except IndexError:
                pass
        if not res:
            return None
        return res

    @staticmethod
    def _is_increasing(iter_values, weak=False):
        """是否存在关键词的行号或者列号递增的情况"""
        if weak:
            min_num = 2
        else:
            min_num = 3
        if isinstance(iter_values, pd.core.series.Series):
            iter_values = iter_values.values
        if iter_values is None:
            return False
        if len(iter_values) < min_num:
            return False
        if not weak:
            for ele in iter_values:
                if ele + 1 in iter_values and ele + 2 in iter_values:
                    return True
        else:
            for ele in iter_values:
                if ele + 1 in iter_values:
                    return True
        return False

    def get_nearest_target_from_one_df(self, df, get_key_loc, key_pattern, get_target_loc, target_pattern, key_loc=None,
                                       target_loc=None):
        """ 从df中获取距离关键词最近的target
        :param df: pandas data frame， info字段中读取到的df
        :param get_key_loc: 获取关键词位置的方法， (df, key_pattern) -> df， 输入df和关键词pattern， 输出关键词位置的df
        :param key_pattern: 关键词pattern
        :param get_target_loc:  获取目标位置的方法 (df, target_pattern) -> df， 输入df和目标pattern， 输出目标位置的df
        :param target_pattern: 目标pattern
        :param key_loc:  预留可选参数，关键词的位置。传入该参数， 就能够不通过get_key_loc获取关键词位置。 df类型，列名[行号，列号]
        :param target_loc: 预留可选参数，目标的位置。传入该参数， 就能够不通过get_target_loc获取目标位置。df类型，列名[行号，列号]
        :return: None/str
        """
        print('aaaa')
        if key_loc and not isinstance(key_loc, pd.DataFrame):  # 检查可选参数
            return None
        if target_loc and not isinstance(target_loc, pd.DataFrame):  # 检查可选参数
            return None
        if not key_loc:
            key_loc = get_key_loc(df, key_pattern)  # 获取关键词位置
        if not target_loc:
            target_loc = get_target_loc(df, target_pattern)  # 获取目标位置
        if key_loc is None or key_loc.shape[0] == 0 or target_loc is None or target_loc.shape[0] == 0:  # 检查位置信息是否有效
            return None
        print(key_loc,target_loc)
        res = []  # 获取的结果
        for cpn_index, cpn_row in target_loc.iterrows():
            for key_index, key_row in key_loc.iterrows():
                # 获取所有关键词位置和所有目标位置之间的距离
                dist_target_key = self.get_distance_data([cpn_row['行号'], cpn_row['列号']],
                                                         [key_row['行号'], key_row['列号']])
                res.append(dist_target_key)  # 存入结果
        if not res:  # 检查
            return None
        res = sorted(res)  # 排序， 按照距离排序
        # 获取距离关键词最近的条数据cell， res为[(距离， 目标位置，关键词位置), (...), (...), ...]
        res_target_cell = self.get_cell_from_locs(df, res[0][1])
        if not res_target_cell:  # 检查空字符串或者None
            return None
        res_target_cell = res_target_cell  # emm 好像是多余的。
        return res_target_cell

    def get_nearest_target_from_dfs(self, dfs, get_key_loc, key_pattern, get_target_loc, target_pattern):
        """ 获得每一个df中距离关键词最近的target
        :param dfs: pandas data frame
        :param get_key_loc: 获取关键词位置的方法， (df, key_pattern) -> df， 输入df和关键词pattern， 输出关键词位置的df
        :param key_pattern:  关键词pattern
        :param get_target_loc:  获取目标位置的方法 (df, target_pattern) -> df， 输入df和目标pattern， 输出目标位置的df
        :param target_pattern:  目标pattern
        :return: 距离关键词最近的目标
        """
        res = []  # 获取的结果
        if dfs is None or not isinstance(dfs, list):  # 检查
            return None
        for df in dfs:  # 遍历每一个df， 获取每个df中距离关键词最近的目标
            res_tem = self.get_nearest_target_from_one_df(df, get_key_loc, key_pattern, get_target_loc, target_pattern)
            if res_tem:
                res += res_tem  # 存入结果
        if not res:  # 检查是否为空列表
            return None
        return res

    def get_surrounding_cell_text_from_one_df(self, df, key_pattern):
        """ 从一个df中获取关键词‘右方’或者‘下方’的内容（cell里面的全部内容） (df, re.pattern) -> None/list
        :param df: 原始df
        :return: None/list
        """
        if df is None:  # 检查df
            return None
        if df.shape[0] == 0:  # 检查是否为空df
            return None
        key_locs = self.get_location(df, key_pattern)  # 获取关键词位置
        if key_locs is not None and key_locs.shape[0] == 0:  # 检查关键词位置是否有效
            return None
        search_direction = self.get_df_type_pro(df, is_weak=True)  # 获取df的类型， 是‘横向查找’还是‘纵向查找’。
        res = []  # 获取的结果
        if search_direction == '纵向查找':
            search_loc = self.get_next_vertical_cell(key_locs, df.shape)  # 获取关键词下方的位置
            res_tem = self.get_cell_from_locs(df, search_loc)  # 获取关键词位置下方的内容
            if res_tem:
                res = res + res_tem  # 存入结果
        elif search_direction == '横向查找':
            search_loc = self.get_next_horizontal_cell(key_locs, df.shape)  # 获取关键词右方的位置
            res_tem = self.get_cell_from_locs(df, search_loc)  # 获取关键词位置右方的内容
            if res_tem:
                res = res + res_tem  # 存入结果
        else:  # 当无法判定df为‘横向查找’还是‘纵向查找’的时候，同时获取关键词右方和下方的内容
            search_loc_ver = self.get_next_vertical_cell(key_locs, df.shape)  # 获取关键词下方的位置
            res_tem_ver = self.get_cell_from_locs(df, search_loc_ver)  # 获取关键词位置下方的内容
            search_hor = self.get_next_horizontal_cell(key_locs, df.shape)  # 获取关键词右方的位置
            res_tem_hor = self.get_cell_from_locs(df, search_hor)  # 获取关键词位置右方的内容
            if res_tem_ver:
                res = res + res_tem_ver  # 存入结果
            if res_tem_hor:
                res = res + res_tem_hor  # 存入结果
        if not res:  # 检查
            return None
        res = list(filter(lambda text: len(str(text)), res))  # 去除空字符串的结果（exist some better ways）
        return res

    def get_surrounding_cell_text_from_dfs(self, dfs, key_pattern):
        """获得关键词（key_pattern）附近的单元格的内容 (pd.DataFrame, re.pattern) -> None/list
        会判断每一个df的类型，该横向查找target，还是纵向查找target， 或者是横向纵向同时查找target.
        :param dfs: list of pandas dataframe
        :param key_pattern: 关键词
        :return: 关键词附近的单元格内容
        """
        if dfs is None:  # 检查dfs是否为None
            return None
        res = []  # 结果暂存
        # 遍历df，获取关键词的‘后一个’位置 或者 ‘下一个’位置 的所有文本信息
        for df in dfs:
            res_tem = self.get_surrounding_cell_text_from_one_df(df, key_pattern)  # 遍历每一个df， 获取关键词‘下右’方文本
            if res_tem:  # 如果存在，那么存入res
                res += res_tem
        if not res:
            return None
        return res

if __name__ == '__main__':
    from extraction import pattern
    d = {'col1': ['第二中标', '第一公司'], 'col2': ['第一中标', '二公司'], 'col3': ['第一中标', '三公司']}
    d = [('第一中标', 'AA公司'), ('中标', 'XXX公司'), ('xx', 'A公司'), ]
    df = pd.DataFrame(data=d)
    dst = Computation(pattern.KEY_PATTERN)
    re_p = re.compile('第[一二三]中')
    # 确保列、行为数字索引:建议之前确保df的行和列都为数字索引
    high, wide = df.shape
    df.columns = range(wide)
    df.index = range(high)
    # 获得关键词的位置
    locs = dst.get_location(df, re_p)
    print(locs)
    print(dst.get_df_type(df, is_weak=True))
    print(df)
    # print(dst.get_next_horizontal_cell(locs, df.shape))
    # print(dst.get_next_vertical_cell(locs, df.shape))
    # print(dst.get_cell_from_locs(df, dst.get_next_horizontal_cell(locs, df.shape)))
    print('正则表达式选择：', dst.get_key_pattern(df.to_string(), pattern.FIRST_KEY_PATTERN, pattern.MORE_KEY_PATTERN, pattern.DISTRIBUTE_KEY_PATTERN))
    print('这是距离关键词最近的公司单个', dst.get_nearest_target_from_one_df(df, dst.get_location, pattern.MORE_KEY_PATTERN, dst.get_location, pattern.COMPANY_PATTERN))
    print('这是距离关键词最近的公司多个', dst.get_nearest_target_from_dfs(list((df, df)), dst.get_location, pattern.MORE_KEY_PATTERN, dst.get_location, pattern.COMPANY_PATTERN))
    print('这是附近查找单个', dst.get_surrounding_cell_text_from_one_df(df, pattern.MORE_KEY_PATTERN))
    print('这是附近查找多个', dst.get_surrounding_cell_text_from_dfs(list((df, df)), pattern.FIRST_KEY_PATTERN))
    # 测试位置
    # import doctest
    # print(doctest.testmod())
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    from algorithm.feature_compute.compute import Computation
    from extraction.bid_data import BidData
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    import os
    from algorithm.bid_information import Information
    os.chdir(os.pardir)
    os.chdir(os.pardir)
    cnn = DataSQL()
    cpt = Computation(pattern.KEY_PATTERN)
    ifm = Information(pattern)
    ids = [4000115, 125452, 8000303, 8000296, 8000147]
    for id in ids:
        bid_data = BidData(cnn, pattern, id, 'stang_bid_new')
        try:
            print(ifm.cpt.get_df_type_pro(bid_data.get_dfs()[0], True))
        except Exception as e:
            print(e)
