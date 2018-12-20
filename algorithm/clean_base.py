from collections import OrderedDict


class Clean(object):

    def __init__(self, pattern):
        self.PU = pattern.PU
        self.PU_FOR_PRICE = pattern.PU_FOR_PRICE
        self.PARENTHESES_AFTER = pattern.PARENTHESES_AFTER
        self.PARENTHESES_PRE = pattern.PARENTHESES_PRE

    @staticmethod
    def remove_duplicates(target_list):
        """ 去除列表中重复的元素，并保留原先的顺序
        :param target_list: 需要去除重复元数的列表
        :return:
        >>> from extraction import pattern
        >>> c = Clean(pattern)
        >>> list(c.remove_duplicates([1,2,3,4,3]))
        [1, 2, 3, 4]
        """
        if target_list is None:
            return None
        if not target_list:
            return None
        target_list = list(OrderedDict.fromkeys(target_list))
        return target_list

    def get_target_list(self, target_list, valid_pattern, get_target_function):
        """获得有效的target列表， (list, re.pattern, func) -> None/list
        :param target_list: 需要清理的target列表
        :param valid_pattern: 列表中每一个字符串必须满足pattern
        :param get_target_function: 对于不符合条件的字符串，清理出有效的target. 输入：字符串， 输出：该字符串中可能有效的target列表
        :return: 有效的target列表 None/list
        """
        pass
