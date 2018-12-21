from collections import OrderedDict
from algorithm.nlp_algorithm.ltp_algorithm import Ner
from algorithm.tag_search import TagSearch
from algorithm.clean_base import Clean
import re


class CleanManager(Clean):

    def get_target_list(self, target_list, invalid_pattern, get_target_function):
        if target_list is None:
            return None
        if not target_list:
            return None
        target_list = self._get_target_pre(target_list, get_target_function)
        valid_list = list(filter(lambda x: self.is_valid(x, invalid_pattern), target_list))
        valid_list = self.remove_duplicates(valid_list)
        return valid_list

    def _get_target_pre(self, target_list, get_target_function):
        res = []
        if target_list is None:
            return res
        if not target_list:
            return res
        for target in target_list:
            res_tem = get_target_function(target)
            if res_tem:
                res += res_tem
        return res


    def is_valid(self, target, invalid_pattern):
        if target is None:
            return False
        if not target:
            return False
        if not isinstance(target, str):
            raise ValueError('clean, target 不是字符串')
        if self.PARENTHESES_PRE.findall(target):
            return False
        if self.PARENTHESES_AFTER.findall(target):
            return False
        if len(target) > 4:
            return False
        if len(target) < 2:
            return False
        for pu in self.PU:
            if pu in target:
                return False
        if invalid_pattern.findall(target):
            return False
        return True



if __name__ == '__main__':
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    import os
    os.chdir(os.pardir)
    ner = Ner()
    c = CleanManager(pattern)
    print(c.get_target_list(['项目经理：李学', '这李济科', 'N法克鱿B公司', '王兵公司：成都科技有限公司', '我叫做张隆'], pattern.MANAGER_PATTERN,
                            ner.get_persons_from_string))