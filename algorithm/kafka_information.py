from algorithm.bid_information import Information
from algorithm.clean_owner import CleanOwner
from algorithm import summary


class KafkaInformation(Information):
    def __init__(self, pattern):
        super(KafkaInformation, self).__init__(pattern)
        self.clean_owner = CleanOwner(pattern)
        self.smy = summary

    def get_owner(self, bid_data_object):
        # 检测单位关键词
        key_pattern = self.pattern.BUILD_OWNER_PATTERN
        res = []
        # df处理
        dfs = self.df_pre.get_all_valid_pandas_df(bid_data_object)

        # 关键词附近
        # 获得关键词（key_pattern）附近的单元格的内容 (pd.DataFrame, re.pattern) -> None/list
        res = self.cpt.get_surrounding_cell_text_from_dfs(dfs, key_pattern)
        # 获得有效的target列表， (list, re.pattern, func) -> None/list
        res = self.clean_owner.get_target_list(res, self.pattern.BUILD_OWNER_CLEAN_PATTERN,
                                                 self.pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                                                 self.ner.get_institution_from_string_checked_len)

        # tag_search
        if not res:
            # 获取p, div, span标签下，出现关键词之后，该标签中可能有效的taget
            res = self.tag_search.get_target(bid_data_object.get_info_soup(), key_pattern,
                                             self.ner.get_institution_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_owner.get_target_list(res, self.pattern.BUILD_OWNER_CLEAN_PATTERN,
                                                     self.pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                                                     self.ner.get_institution_from_string_checked_len)

        # sequence_search
        if not res:
            # 获取每一个文本小‘片段’中，出现关键词之后，该标签中可能有效的taget
            res = self.sequence_search.get_target(bid_data_object.get_info_sequence(), key_pattern,
                                                  self.ner.get_institution_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_owner.get_target_list(res, self.pattern.BUILD_OWNER_CLEAN_PATTERN,
                                                     self.pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                                                     self.ner.get_institution_from_string_checked_len)

        # version2 采用第二种提取的方式，以下使用地名实体+其他实体的方式进行提取 采用ner.get_owner_from_string_checked_len方法
        # tag_search
        if not res:
            # 获取p, div, span标签下，出现关键词之后，该标签中可能有效的taget
            res = self.tag_search.get_target(bid_data_object.get_info_soup(), key_pattern,
                                             self.ner.get_owner_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_owner.get_target_list(res, self.pattern.BUILD_OWNER_CLEAN_PATTERN,
                                                   self.pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                                                   self.ner.get_institution_from_string_checked_len)

        # sequence_search
        if not res:
            # 获取每一个文本小‘片段’中，出现关键词之后，该标签中可能有效的taget
            res = self.sequence_search.get_target(bid_data_object.get_info_sequence(), key_pattern,
                                                  self.ner.get_owner_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_owner.get_target_list(res, self.pattern.BUILD_OWNER_CLEAN_PATTERN,
                                                   self.pattern.INVALID_BUILD_OWNER_CLEAN_PATTERN,
                                                   self.ner.get_institution_from_string_checked_len)

        if not res:
            return None
        return ' '.join(res)

    def get_summary(self, bid_data_object):
        # 给与p标签结束后，加一个换行符。
        info_text = self.smy.add_new_line_before_end_ptag(bid_data_object, self.pattern.P_TAG_PATTERN)
        # 去除文本中的空格
        info_text_with_out_space = self.pattern.SPACE_PATTERN.sub('', info_text)
        if not info_text_with_out_space:
            return None
        # 判断文本中是否出现项目概况这类的关键词
        if not self.pattern.SUMMARY_INDEX_FULL_TEXT_PATTERN.findall(info_text_with_out_space):
            return None
        else:
            # 获得分割的文本
            info_sequence = self.pattern.SUMMARY_SPLIT_PATTERN.split(info_text_with_out_space)
            # 获得关键词位置
            key_locs = self.smy.get_key_loc(info_sequence, self.pattern.SUMMARY_INDEX_PATTERN)
            # 获取建设规模
            summary_res = self.smy.get_summary(info_sequence, key_locs, self.pattern.SUMMARY_CONTAIN_PATTERN,
                                               self.pattern.SUMMARY_REMOVE_NUM_PATTERN)
            if summary_res:
                return summary_res
            else:
                return None

    def get_information(self, bid_data_object):
        # 如果是中标数据那么返回第一中标单位和中标金额等信息
        if bid_data_object.cate_id == 2 and bid_data_object.is_valid():
            first_bid_company = self.get_first_company(bid_data_object)
            first_bid_money = ''

            invest_money = ''
            summary = ''
            build_owner = ''
            return first_bid_company, first_bid_money, invest_money, summary, build_owner

        # cate_id为0， 表示为拟建数据。则返回其投资额， 项目概况，建设单位等字段信息
        if bid_data_object.cate_id == 0 and bid_data_object.is_valid():
            # 拟建数据中也会有中标数据，如果是中标数据，则提取其字段
            if bid_data_object.is_valid_bid():
                first_bid_company = self.get_first_company(bid_data_object)
                first_bid_money = ''
            else:
                first_bid_company = ''
                first_bid_money = ''
            invest_money = ''
            summary = self.get_summary(bid_data_object)
            build_owner = self.get_owner(bid_data_object)
            return first_bid_company, first_bid_money, invest_money, summary, build_owner

        return '', '', '', '', ''
