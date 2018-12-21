from algorithm.feature_compute.compute import Computation
import pandas as pd

class DistanceSearch(object):

    def __init__(self):
        pass

    def get_nearest_target_from_one_df(self, df, get_key_loc, key_pattern, get_target_loc, target_pattern, key_loc=None, target_loc=None):
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
                dist_target_key = self.cpt.get_distance_data([cpn_row['行号'], cpn_row['列号']], [key_row['行号'], key_row['列号']])
                res.append(dist_target_key)
        if not res:
            return None
        # 排序后，返回距离最短的那组数据
        res = sorted(res)
        # 获取距离关键词最近的条数据cell
        res_cpn_cell = self.cpt.get_cell_from_locs(df, res[0][1])
        if not res_cpn_cell:
            return None
        res_cpn_cell = res_cpn_cell
        return res_cpn_cell

    def get_first_company(self, dfs):
        """
        :param dfs:
        :return:
        """
        res = []
        if dfs is None or not dfs or not isinstance(dfs, list):
            return None
        for df in dfs:
            res_tem = self.get_first_company_one_df(df)
            if res_tem:
                res += res_tem
        if not res:
            return None
        return res




