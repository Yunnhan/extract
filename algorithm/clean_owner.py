from algorithm.clean_base import Clean


class CleanOwner(Clean):

    def get_target_list(self, target_list, valid_pattern, invalid_pattern, get_target_function):
        """获得有效的target列表， (list, re.pattern, func) -> None/list
        :param target_list: 需要清理的target列表
        :param valid_pattern: 列表中每一个字符串必须满足pattern
        :param invalid_pattern: 列表中每一个字符串必须满足pattern
        :param get_target_function: 对于不符合条件的字符串，清理出有效的target. 输入：字符串， 输出：该字符串中可能有效的target列表
        :return: 有效的target列表 None/list
        """
        if target_list is None:
            return None
        if not target_list:
            return None
        valid_list = list(filter(lambda x: self.is_valid(x, valid_pattern, invalid_pattern), target_list))
        invalid_list = list(filter(lambda x: not self.is_valid(x, valid_pattern, invalid_pattern), target_list))
        valid_list_from_invalid = self.get_target_from_invalid_list(invalid_list, valid_pattern, invalid_pattern, get_target_function)
        if valid_list is not None and valid_list_from_invalid:
            valid_list += valid_list_from_invalid
        valid_list = self.remove_duplicates(valid_list)
        return valid_list

    def get_target_from_invalid_list(self, invalid_list, valid_pattern, invalid_pattern, get_target_function):
        if invalid_list is None or not invalid_list:
            return None
        valid_list_from_bad = self.get_target_via_function(invalid_list, get_target_function)
        if valid_list_from_bad is not None and valid_list_from_bad:
            valid_list_from_bad = list(filter(lambda x: self.is_valid(x, valid_pattern, invalid_pattern), valid_list_from_bad))
        return valid_list_from_bad

    def is_valid(self, target, valid_pattern, invalid_pattern):
        if target is None:
            return False
        if not target:
            return False
        if not isinstance(target, str):
            raise ValueError('clean, target 不是字符串')
        # 解决10170674这类型问题 杭州）股份有限公司
        if self.PARENTHESES_PRE.findall(target) and not self.PARENTHESES_AFTER.findall(target):
            return False
        if self.PARENTHESES_AFTER.findall(target) and not self.PARENTHESES_PRE.findall(target):
            return False
        if len(target) > 22:
            return False
        if len(target) < 6:
            return False
        for pu in self.PU:
            if pu in target:
                return False
        if invalid_pattern.findall(target):
            return False
        if valid_pattern.findall(target):
            return True
        return False

    def get_target_via_function(self, target_list, get_target_function):
        if target_list is None or not target_list:
            return None
        res = []
        for target in target_list:
            res_tem = get_target_function(target)
            if res_tem is not None and res_tem:
                res += res_tem
        if not res:
            return None
        return res


if __name__ == '__main__':
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    import os
    os.chdir(os.pardir)
    ner = Ner()
    c = CleanOwner(pattern)
    print(c.get_target_list(['重庆市渝兴建设投资有限公司 招标代理机构', '重庆市渝兴建设投资有限公司', '招标代理机构'],
                            pattern.BUILD_OWNER_CLEAN_PATTERN, pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                            ner.get_institution_from_string_checked_len))