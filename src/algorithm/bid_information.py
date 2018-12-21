from algorithm.create_df.data_frame_main import DataFramePre
from algorithm.feature_compute.compute import Computation
from algorithm.feature_compute.compute_manager import ComputationManager
from algorithm.clean_company import CleanCompany
from algorithm.clean_manager import CleanManager
# from algorithm.distance_search import DistanceSearch
from algorithm.tag_search import TagSearch
from algorithm.sequence_search import SequenceSearch
# from algorithm.nlp_algorithm.ltp_algorithm import Ner
from extraction import pattern
from algorithm.clean_owner import CleanOwner
from algorithm import summary

class Information(object):

    def __init__(self, pattern):
        # 自然语言处理程序，可以判断一句话中的机构或者是人名
#         self.ner = Ner()
        # df预处理程序
        self.df_pre = DataFramePre(pattern)
        # df提取相关程序
        self.cpt = Computation(pattern.KEY_PATTERN)  # 不用修改参数
        # 数据清理相关程序
        self.clean_company = CleanCompany(pattern)
        # 项目经理数据清洗相关程序
        self.clean_manager = CleanManager(pattern)
        # 项目经理df提取相关程序
#         self.cpt_manager = ComputationManager(pattern, self.ner, self.clean_manager)
        # 经常会使用到的正则表达式
        self.pattern = pattern
        # 标签提取相关程序
        self.tag_search = TagSearch()
        # 分‘段’提取相关程序
        self.sequence_search = SequenceSearch(pattern)
        # 业主清理程序
        self.clean_owner = CleanOwner(pattern)
        # 建设概况
        self.smy = summary

    def get_first_company(self, bid_data_object):
        """获取第一中标单位
        :param bid_data_object: BidData 对象
        :return: 第一中标单位， 空格分开
        """
        # 获取第一中标候选人的关键词正则表达式。如果文本中拥有干扰的关键词， 那么就使用一种更加‘精确’的关键词，否则就使用一种更加
        # ‘广泛’的关键词
        key_pattern = self.cpt.get_key_pattern(bid_data_object.get_info_text(), pattern.FIRST_KEY_PATTERN, pattern.MORE_KEY_PATTERN,
                                               pattern.DISTRIBUTE_KEY_PATTERN)
        res = []
        # df处理
        dfs = self.df_pre.get_all_valid_pandas_df(bid_data_object)

        # 关键词附近
        # 获得关键词（key_pattern）附近的单元格的内容 (pd.DataFrame, re.pattern) -> None/list
        res = self.cpt.get_surrounding_cell_text_from_dfs(dfs, key_pattern)
        # 获得有效的target列表， (list, re.pattern, func) -> None/list
        res = self.clean_company.get_target_list(res, self.pattern.KEY_COMPANY_PATTERN, self.ner.get_institution_from_string_checked_len)

        # 最近距离
        if not res:
            # 获得每一个df中距离关键词最近的target
            res = self.cpt.get_nearest_target_from_dfs(dfs, self.cpt.get_location, key_pattern, self.cpt.get_location,
                                                       self.pattern.COMPANY_PATTERN)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_company.get_target_list(res, self.pattern.KEY_COMPANY_PATTERN, self.ner.get_institution_from_string_checked_len)

        # tag_search
        if not res:
            # 获取p, div, span标签下，出现关键词之后，该标签中可能有效的target
            res = self.tag_search.get_target(bid_data_object.get_info_soup(), key_pattern, self.ner.get_institution_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_company.get_target_list(res, self.pattern.KEY_COMPANY_PATTERN, self.ner.get_institution_from_string_checked_len)

        # sequence_search
        if not res:
            # 获取每一个文本小‘片段’中，出现关键词之后，该标签中可能有效的target
            res = self.sequence_search.get_target(bid_data_object.get_info_sequence(), key_pattern, self.ner.get_institution_from_string_checked_len)
            # 获得有效的target列表， (list, re.pattern, func) -> None/list
            res = self.clean_company.get_target_list(res, self.pattern.KEY_COMPANY_PATTERN, self.ner.get_institution_from_string_checked_len)
        if not res:
            return None
        return ' '.join(res)

    def get_manager(self, bid_data_object, first_bid_company):
        key_pattern = pattern.RESPONSIBLE_PATTERN
        res = []
        # df处理
        dfs = self.df_pre.get_all_valid_pandas_df(bid_data_object)
        # 关键词附近

        res = self.cpt.get_surrounding_cell_text_from_dfs(dfs, key_pattern)
        res = self.clean_manager.get_target_list(res, self.pattern.MANAGER_PATTERN,
                                         self.ner.get_persons_from_string)
        # 最近距离
        if not res and first_bid_company:
            # 在项目经理方面，不是查找距离关键词最近，而是查找距离第一中标候选人最近的。
            res = self.cpt_manager.get_nearest_target_from_dfs(self.df_pre.get_all_valid_pandas_df(bid_data_object),
                                                               self.cpt_manager.get_location, first_bid_company,
                                                               self.cpt_manager.get_manager_loc, None)
            res = self.clean_manager.get_target_list(res, pattern.MANAGER_PATTERN, self.ner.get_persons_from_string)

        # tag_search
        if not res:
            res = self.tag_search.get_target(bid_data_object.get_info_soup(), key_pattern,
                                             self.ner.get_persons_from_string)
            res = self.clean_manager.get_target_list(res, self.pattern.MANAGER_PATTERN,
                                             self.ner.get_persons_from_string)
        # sequence_search
        if not res:
            res = self.sequence_search.get_target(bid_data_object.get_info_sequence(), key_pattern,
                                                  self.ner.get_persons_from_string)
            res = self.clean_manager.get_target_list(res, self.pattern.MANAGER_PATTERN,
                                             self.ner.get_persons_from_string)
        if not res:
            return None
        # 第一中标和第二中标的项目负责人都是具有相同的关键词
        return res[0]

    def get_first_bidmoney(self, bid_data_obj):
        print('it works!', bid_data_obj.id)
        # your main code here
        return None

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
        # 检查是否为有效的中标数据
        if not bid_data_object.is_valid_bid():
            return None, None
        # debug 代码
        # first_bid_company = ''
        first_bid_company = self.get_first_company(bid_data_object)
        # debug 代码
        # manager = ''
        manager = self.get_manager(bid_data_object, first_bid_company)
        return first_bid_company, manager

