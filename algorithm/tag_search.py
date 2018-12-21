from bs4 import BeautifulSoup
from algorithm.feature_compute.compute import Computation
from algorithm.nlp_algorithm.ltp_algorithm import Ner
from algorithm.create_df.read_data_lib.data_base import DataSQL
import re

class TagSearch(object):

    def __init__(self):
        pass

    def get_target(self, info_soup, key_pattern, get_target_function):
        """获取p, div, span标签下，出现关键词之后，该标签中可能有效的taget
        :param info_soup:  Beautiful_soup 对象
        :param key_pattern: 关键词
        :param get_target_function: 从含有关键词的字符串中，提取出有效的target方法，输入：字符串，输入：有效targe列表
        :return: 有效的target列表
        """
        if info_soup is None:
            return None

        text_segs = info_soup.find_all(name=['p', 'div', 'span'])
        res = []
        for each in text_segs:
            each_text = each.text
            # 为了解决id为11269279这样的情况，所有的信息为一个大的txt， 所以必须要限定文本的长度。
            if key_pattern.findall(each_text) and len(str(each_text)) < 50:
                target = get_target_function(each.text)
                if target is not None and target:
                    res += target
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
    ner = Ner()
    bid_data = BidData(cnn, pattern, 23540, 'stang_bid_new')
    tag_search = TagSearch()
    print(tag_search.get_target(bid_data.get_info_soup(), pattern.FIRST_KEY_PATTERN, ner.get_institution_from_string))
