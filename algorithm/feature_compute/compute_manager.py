from algorithm.feature_compute.compute import Computation
import pandas as pd

class ComputationManager(Computation):

    def __init__(self, pattern, ner, clean_manager):
        super(ComputationManager, self).__init__(pattern.KEY_PATTERN)
        self.ner = ner
        self.pattern = pattern
        self.clean_manager = clean_manager

    def get_manager_loc(self, df, target_pattern):
        if df is None:
            return None
        if not isinstance(df, pd.DataFrame):
            return None

        df_tem = df.copy()
        for column in df_tem.columns:
            df_tem[column] = df_tem[column].apply(self._exist_name)
        locs = self.get_location(df_tem, self.pattern.PERSON_EXIST_FLAG)
        return locs

    def _exist_name(self, string):
        if string is None:
            return 'False'
        if not isinstance(string, str):
            return 'False'
        names = self.ner.get_persons_from_string(string)
        names = self.clean_manager.get_target_list(names, self.pattern.MANAGER_PATTERN,
                                                   self.ner.get_persons_from_string)
        if names is not None:
            return 'True'
        else:
            return 'False'

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
        # 当df中没有包含‘项目经理’等关键词的时候，直接返回None
        if not self.pattern.RESPONSIBLE_PATTERN_FOR_MANAGER_COMPUTE.findall(df.to_string()):
            return None
        if key_loc and not isinstance(key_loc, pd.DataFrame):
            return None
        if target_loc and not isinstance(target_loc, pd.DataFrame):
            return None
        if not key_loc:
            key_loc = get_key_loc(df, key_pattern)
        if not target_loc:
            target_loc = get_target_loc(df, target_pattern)
        if key_loc is None or key_loc.shape[0] == 0 or target_loc is None or target_loc.shape[0] == 0:
            return None
        res = []
        for cpn_index, cpn_row in target_loc.iterrows():
            for key_index, key_row in key_loc.iterrows():
                dist_target_key_targetrow_targetcolumn = self.get_distance_data([cpn_row['行号'], cpn_row['列号']],
                                                              [key_row['行号'], key_row['列号']])
                dist_target_key_targetrow_targetcolumn = list(dist_target_key_targetrow_targetcolumn)
                dist_target_key_targetrow_targetcolumn.append(cpn_row['行号'])
                dist_target_key_targetrow_targetcolumn.append(cpn_row['列号'])
                res.append(tuple(dist_target_key_targetrow_targetcolumn))
        if not res:
            return None
        # 排序后，返回距离最短的那组数据, 如果距离相同，返回行号小的数据，如果行号相同，返回列号最小的数据
        res = sorted(res, key=lambda x: (x[0], x[3], x[4]))
        # 获取距离关键词最近的条数据cell
        res_target_cell = self.get_cell_from_locs(df, res[0][1])
        if not res_target_cell:
            return None
        res_target_cell = res_target_cell
        return res_target_cell

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


if __name__ == '__main__':
    from extraction import pattern
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    from algorithm.feature_compute.compute import Computation
    from extraction.bid_data import BidData
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    import os
    from algorithm.create_df.data_frame_main import DataFramePre
    from algorithm.bid_information import Information
    from algorithm.clean_manager import CleanManager

    os.chdir(os.pardir)
    os.chdir(os.pardir)
    cnn = DataSQL()
    cpt = Computation(pattern.KEY_PATTERN)
    ner = Ner()
    clean_manager = CleanManager(pattern)
    cpt_manager = ComputationManager(pattern, ner, clean_manager)
    df_pre = DataFramePre(pattern)
    ifm = Information(pattern)
    ids = [8905, 8896, 8898, 4000115, 125452, 8000303, 8000296, 8000147]
    for id in ids:
        data = BidData(cnn, pattern, id, 'stang_bid_new')
        # try:
        # print(cpt_manager.get_manager_loc(df_pre.get_all_valid_pandas_df(data)[0], None))
        first_bidcompany = ifm.get_information(data)[0]
        if first_bidcompany:
            # 在项目经理方面，不是查找距离关键词最近，而是查找距离第一中标候选人最近的。
            res = cpt_manager.get_nearest_target_from_dfs(df_pre.get_all_valid_pandas_df(data),
                                                          cpt_manager.get_location, first_bidcompany,
                                                          cpt_manager.get_manager_loc, None)
            res = clean_manager.get_target_list(res, pattern.MANAGER_PATTERN, ner.get_persons_from_string)
        if res:
            print(res[0], first_bidcompany)

        # except Exception as e:
        #     print(e)