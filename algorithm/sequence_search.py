from algorithm.tag_search import TagSearch
from algorithm.create_df.read_data_lib.data_base import DataSQL
from algorithm.feature_compute.compute import Computation
# from algorithm.clean_old import Clean
from bs4 import BeautifulSoup
import re


class SequenceSearch(object):

    def __init__(self, pattern):
        self.pattern = pattern

    def _get_key_loc(self, info_sequence, pattern):
        """ 获取关键词位置索引
        :param info_sequence: 切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param pattern: 关键词
        :return: 位置（索引） list of int
        """
        if info_sequence is None or not info_sequence:
            return None
        if not isinstance(info_sequence, list):
            raise ValueError('info_sequence is not a list')
        key_locs = []
        for i in range(len(info_sequence)):
            # 去除每一个‘小片段’文本中的空格
            each = self.pattern.SPACE_PATTERN.sub('', str(info_sequence[i]))
            if pattern.findall(each) and  len(each) < 800:  # 判断条件预留 长度800
                key_locs.append(i)
        if not key_locs:
            return None
        return key_locs

    @staticmethod
    def _get_next_loc(info_sequence, key_locs):
        """获取关键词下一个位置
        :param info_sequence:  切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param key_locs: 关键词的位置（索引） list of int
        :return: 下一个位置（索引） list of int
        """
        if info_sequence is None or not info_sequence:
            return None
        if not isinstance(info_sequence, list):
            raise ValueError('info_sequence is not a list')
        if key_locs is None or not key_locs:
            return None
        next_locs = []
        for loc in key_locs:
            next_locs.append(loc + 1)
        max_len = len(info_sequence)
        next_locs = list(filter(lambda loc: loc < max_len, next_locs))
        if not next_locs:
            return None
        return next_locs

    def _find_target_in_key_position(self, info_sequence, key_locs, get_target_function):
        """从包含关键词位置的，获得target
        :param info_sequence: 切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param key_locs: list of int (包含关键词的位置索引)
        :param get_target_function: 从含有关键词的字符串中，提取出有效的target方法，输入：字符串，输入：有效targe列表
        :return: 有效target列表
        """
        if not key_locs:
            return None
        key_infos = self._get_info_from_locs(info_sequence, key_locs)
        if not key_locs:
            return None
        res = []
        for info in key_infos:
            res_tem = get_target_function(info)
            if res_tem:
                res += res_tem
        if not res:
            return None
        return res

    @staticmethod
    def _get_info_from_locs(info_sequence, locs):
        """获取‘索引’位置的文本信息
        :param info_sequence: 切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param locs: list of int 需要获取信息的位置的index
        :return: string（index 位置的文本）
        """
        if not locs:
            return None
        if not isinstance(locs, list) and not isinstance(locs, tuple):
            raise ValueError('loc is not a iter')
        if not isinstance(info_sequence, list):
            raise ValueError('info_sequence is not a list')
        res = []
        for loc in locs:
            res.append(info_sequence[loc])
        if not res:
            return None
        return res

    def _find_target_in_next_position(self, info_sequence, next_loc, get_target_function):
        """从包含关键词位置的，下一个位置获得target
        :param info_sequence: 切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param next_loc: list of int (包含关键词的下一个位置)
        :param get_target_function: 从可能的字符串中，提取出有效的target方法，输入：字符串，输入：有效targe列表
        :return: 有效target列表
        """
        if not next_loc:
            return None
        key_infos = self._get_info_from_locs(info_sequence, next_loc)
        if not next_loc:
            return None
        res = []
        for info in key_infos:
            res_tem = get_target_function(info)
            if res_tem:
                res += res_tem
        if not res:
            return None
        return res

    def get_target(self, info_sequence, key_pattern, get_target_function):
        """ 获取每一个文本小‘片段’中，出现关键词之后，该标签中可能有效的taget
        :param info_sequence:  切分好的文本小段，使用空格、换行符、冒号等将文本分好的list
        :param key_pattern:  关键词
        :param get_target_function: 从含有关键词的字符串中，提取出有效的target方法，输入：字符串，输入：有效targe列表
        :return: 有效target列表
        """
        key_locs = self._get_key_loc(info_sequence, key_pattern)
        next_locs = self._get_next_loc(info_sequence, key_locs)
        res = []
        res_key_tem = self._find_target_in_key_position(info_sequence, key_locs, get_target_function)
        res_next_tem = self._find_target_in_next_position(info_sequence, next_locs, get_target_function)
        if res_key_tem:
            res += res_key_tem
        if res_next_tem:
            res += res_next_tem
        if not res:
            return None
        return res


if __name__ == '__main__':
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    from algorithm.feature_compute.compute import Computation
    from extraction.bid_data import BidData
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    import os

    os.chdir(os.pardir)
    cnn = DataSQL()
    bid_data = BidData(cnn, pattern, 23540, 'stang_bid_new')
    cpt = Computation(pattern.KEY_PATTERN)
    ner = Ner()
    sequence_search = SequenceSearch(pattern)
    print(sequence_search.get_target(bid_data.get_info_sequence(), pattern.FIRST_KEY_PATTERN, ner.get_institution_from_string))



