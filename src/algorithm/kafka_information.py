from algorithm.bid_information import Information
import os
import pandas as pd


class KafkaInformation(Information):
    def __init__(self, pattern):
        super(KafkaInformation, self).__init__(pattern)
        zj_cpn_name_path = os.path.join('algorithm/sub_data', 'zhongjiao_company_name.xlsx')
        self.zj_cpn_names_without_zj = pd.read_excel(zj_cpn_name_path)['name_without_zj'].tolist()

    def get_first_company_zj(self, bid_data_object):  # 专门为中交集团处理公司名称的程序
        first_bid_company = self.get_first_company(bid_data_object)  # 获取原来程序提取的第一中标候选人
        # 当文本中出现了中交两个字的时候
        if first_bid_company and self.pattern.ZHONGJIAO.findall(bid_data_object.get_info_text()):
            cpns = first_bid_company.split()  # split为列表
            for i in range(len(cpns)):
                if cpns[i] in self.zj_cpn_names_without_zj:  # 中交的公司， 但是缺少了中交两个字
                    cpns[i] = '中交' + cpns[i]  # 添加中交
            return ' '.join(cpns)
        return first_bid_company

    def get_information(self, bid_data_object):
        # 如果是中标数据那么返回第一中标单位和中标金额等信息
        if bid_data_object.cate_id == 2 and bid_data_object.is_valid():
            first_bid_company = self.get_first_company_zj(bid_data_object)
            first_bid_money = ''

            invest_money = ''
            summary = ''
            build_owner = ''
            return first_bid_company, first_bid_money, invest_money, summary, build_owner

        # cate_id为0， 表示为拟建数据。则返回其投资额， 项目概况，建设单位等字段信息
        if bid_data_object.cate_id == 0 and bid_data_object.is_valid():
            # 拟建数据中也会有中标数据，如果是中标数据，则提取其字段
            if bid_data_object.is_valid_bid():
                first_bid_company = self.get_first_company_zj(bid_data_object)
                first_bid_money = ''
            else:
                first_bid_company = ''
                first_bid_money = ''
            invest_money = ''
            summary = self.get_summary(bid_data_object)
            build_owner = self.get_owner(bid_data_object)
            return first_bid_company, first_bid_money, invest_money, summary, build_owner

        return '', '', '', '', ''
