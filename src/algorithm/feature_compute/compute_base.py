import numpy as np
import pandas as pd
import re
import warnings
from extraction import pattern


class Computation(object):

    def __init__(self, df_type_key_pattern):
        warnings.filterwarnings("ignore", 'This pattern has match groups')
        # 这些关键词用以确定该表格是纵向还是横向排列的表格
        self.KEY_PATTERN = df_type_key_pattern

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
        if info is None:
            return less_key_pattern
        if bad_key_pattern.findall(info):
            return less_key_pattern
        else:
            return more_key_pattern

    @staticmethod
    def get_distance(loc_a, loc_b):
        """获取曼哈顿距离， 横向距离加上纵向距离(iter, iter) -> int
        >>> from extraction import pattern
        >>> c = Computation(pattern.KEY_PATTERN)
        >>> c.get_distance([0,1], [1,0])
        2
        >>> c.get_distance([1,1], [2, 2])
        2
        """
        if not loc_a or not loc_b:
            return None
        try:
            loc_a = [int(ele) for ele in loc_a]
            loc_b = [int(ele) for ele in loc_b]
        except ValueError:
            return None
        loc_a = np.array(loc_a)
        loc_b = np.array(loc_b)
        dst = np.sum(np.abs(loc_a - loc_b))
        return dst

    def get_distance_data(self, loc_a, loc_b):
        """获得距离信息 （iter, iter） -> (int, iter, iter)
        :param loc_a:
        :param loc_b:
        :return:
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
        high, wide = df.shape
        # 确保列、行为数字索引:建议之前确保df的行和列都为数字索引
        # df.columns = range(wide)
        # df.index = range(high)
        # todo 使用where方法，结合正则表达式，将符合条件的cell转换为True， 然后返回True的索引
        # 遍历每一列，如果找到关键词所在的位置索引
        for col in df:
            row_index = df.ix[df[col].str.contains(key_pattern), col].index
            if row_index.shape[0]:
                [res.append((ele, col)) for ele in row_index]
        if not res:
            return None
        if res:
            res_df = pd.DataFrame(res, columns=['行号', '列号'])
        return res_df


    def _get_df_type(self, key_locations, is_weak):
        """获得df的查找方向，是横向查找还是纵向查找
        同一行:关键词的列号序号呈现+1增长，那么为纵向查找。
        同一列:关键词的列号行号呈现+1增长，那么为横向查找。
        :param key_locations:
        :param is_weak:
        :return:
        """
        if key_locations is None:
            return None
        if key_locations.shape[0] == 0:
            return None
        # key_location 可以是df
        loc_df = pd.DataFrame(key_locations, columns=['行号', '列号'])
        # 相同的一行，如果关键词呈现按照列号递增的情况。那么应该纵向查找 ex： 第一中标人 第二中标人 第三中标人
        for row, frame in loc_df.groupby('行号'):
            if self._is_increasing(frame['列号'], is_weak):
                return '纵向查找'
        # 相同的一列，如果关键词呈现按照行号递增的情况。那么应该横向查找
        # ex:  第一中标人
        #      第二中标人
        #      第三中标人
        for col, frame in loc_df.groupby('列号'):
            if self._is_increasing(frame['行号'], is_weak):
                return '横向查找'
        return None

    def _get_df_type_pro(self, key_locations, is_weak):
        """获取df的查找方向，是横向查找还是纵向查找。
        同一行:关键词的列号序号呈现+1增长，那么为纵向查找。
        不同列：关键词的行号呈现+1增长，那么为横向查找。
        :param key_locations:
        :param is_weak:
        :return:
        """
        if key_locations is None:
            return None
        if key_locations.shape[0] == 0:
            return None
        loc_df = pd.DataFrame(key_locations, columns=['行号', '列号'])
        # 相同的一行，如果关键词呈现按照列号递增的情况。那么应该纵向查找 ex： 第一中标人 第二中标人 第三中标人
        for row, frame in loc_df.groupby('行号'):
            if self._is_increasing(frame['列号'], is_weak):
                return '纵向查找'
        if self._is_increasing(loc_df['行号'], is_weak):
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
        :param df:
        :param is_weak:
        :return:
        """
        locs = self.get_location(df, self.KEY_PATTERN)
        return self._get_df_type_pro(locs, is_weak)

    @staticmethod
    def get_next_horizontal_cell(locs, df_shape=None):
        """获取下一个横向位置， 注意：获得的位置不会超出data frame -> None/df
        :param locs: pandas data frame column=['行号', '列号']
        :return: data frame
        """
        if locs is None:
            return None
        if not isinstance(locs, pd.DataFrame) or not isinstance(locs, np.ndarray):
            try:
                locs = pd.DataFrame(locs, columns=['行号', '列号'])
            # todo 异常捕获，以及存储
            except:
                pass
        # data frame 是可变类型, 会在原来变量的位置上修改，这里copy处理，不需要inplace操作
        locs = locs.copy()
        locs['列号'] = locs['列号'] + 1
        if df_shape:
            locs = locs[locs['列号'] < df_shape[1]]
        return locs

    @staticmethod
    def get_next_vertical_cell(locs, df_shape=None):
        """获取下一个纵向位置， 注意：获得的位置不会超出data frame -> None/df
        :param locs: pandas data frame column=['行号', '列号']
        :return: data frame
        """
        if locs is None:
            return None
        if not isinstance(locs, pd.DataFrame) or not isinstance(locs, np.ndarray):
            try:
                locs = pd.DataFrame(locs, columns=['行号', '列号'])
            # todo 异常捕获，以及存储
            except:
                pass
        # data frame 是可变类型, 会在原来变量的位置上修改，这里copy处理，不需要inplace操作
        locs = locs.copy()
        locs['行号'] = locs['行号'] + 1
        if df_shape:
            locs = locs[locs['行号'] < df_shape[0]]
        return locs

    @staticmethod
    def get_cell_from_locs(df, locs_df):
        """ 从位置df获取， 需要的cell内容 -> None/list
        :param df: 原始df
        :param locs_df: 原始df中的位置信息（类型为df）
        :return: 原始df中，这些位置的内容
        """
        if locs_df is None:
            return None
        if not isinstance(df, pd.DataFrame):
            raise ValueError('参数不是Data frame')
        if not isinstance(locs_df, pd.DataFrame):
            try:
                if not isinstance(locs_df, list) and not isinstance(locs_df, tuple):
                    return None
                locs_df = pd.DataFrame(list((tuple(locs_df), )), columns=['行号', '列号'])
            except:
                raise ValueError('位置参数不正确')
        if locs_df.shape[0] == 0:
            return None
        res = []
        for index, row in locs_df.iterrows():
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

        :param df:
        :param get_key_loc:
        :param key_pattern:
        :param get_target_loc:
        :param target_pattern:
        :param key_loc:
        :param company_loc:
        :return:
        """
        if not isinstance(key_loc, pd.DataFrame):
            return None
        if not isinstance(target_loc, pd.DataFrame):
            return None
        pass

    def get_nearest_target_from_dfs(self, dfs, get_key_loc, key_pattern, get_target_loc, target_pattern):
        """ 获得每一个df中距离关键词最近的target
        :param dfs:
        :param get_key_loc:
        :param key_pattern:
        :param get_target_loc:
        :param target_pattern:
        :return:
        """
        res = []
        if dfs is None or not isinstance(dfs, list):
            return None
        for df in dfs:
            res_tem = self.get_nearest_target_from_one_df(df, get_key_loc, key_pattern, get_target_loc, target_pattern)
            if res_tem:
                res += res_tem
        if not res:
            return None
        return res

    def get_surrounding_cell_text_from_one_df(self, df, key_pattern):
        """ 从一个df中获取 第一需要的信息（cell里面的全部内容） (df, re.pattern) -> None/list
        :param df: 原始df
        :return: None/list
        """
        if df is None:
            return None
        if df.shape[0] == 0:
            return None
        key_locs = self.get_location(df, key_pattern)
        if key_locs is not None and key_locs.shape[0] == 0:
            return None
        search_direction = self.get_df_type_pro(df, is_weak=True)
        res = []
        if search_direction == '纵向查找':
            search_loc = self.get_next_vertical_cell(key_locs, df.shape)
            res_tem = self.get_cell_from_locs(df, search_loc)
            if res_tem:
                res = res + res_tem
        elif search_direction == '横向查找':
            search_loc = self.get_next_horizontal_cell(key_locs, df.shape)
            res_tem = self.get_cell_from_locs(df, search_loc)
            if res_tem:
                res = res + res_tem
        else:
            search_loc_ver = self.get_next_vertical_cell(key_locs, df.shape)
            res_tem_ver = self.get_cell_from_locs(df, search_loc_ver)
            search_hor = self.get_next_horizontal_cell(key_locs, df.shape)
            res_tem_hor = self.get_cell_from_locs(df, search_hor)
            if res_tem_ver:
                res = res + res_tem_ver
            if res_tem_hor:
                res = res + res_tem_hor
        if not res:
            return None
        res = list(filter(lambda text: len(str(text)), res))
        return res

    def get_surrounding_cell_text_from_dfs(self, dfs, key_pattern):
        """获得关键词（key_pattern）附近的单元格的内容 (pd.DataFrame, re.pattern) -> None/list
        会判断每一个df的类型，该横向查找target，还是纵向查找target， 或者是横向纵向同时查找target.
        :param dfs: pandas dataframe
        :param key_pattern: 关键词
        :return: 关键词附近的单元格内容
        """
        if dfs is None:
            return None
        res = []
        # 遍历df，从关键词的‘后一个’位置 或者 ‘下一个’位置 提取需要的公司
        for df in dfs:
            res_tem = self.get_surrounding_cell_text_from_one_df(df, key_pattern)
            if res_tem:
                res += res_tem
        if not res:
            return None
        return res


