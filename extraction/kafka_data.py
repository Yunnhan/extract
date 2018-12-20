from extraction.data import Data


class KafkaData(Data):

    def __init__(self, data, pattern):
        # 从kafak读取的数据， data是一个字典
        self.data = data
        self.pattern = pattern

        self.table_name = None
        self.id = None
        self.title = None
        self.info = None
        self.cate_id = None  # 0表示拟建项目 1招标项目 2中标项目
        self.label = None

        self.dfs = None
        self.info_sequence = None
        self.tags_sequence = None
        self.info_text = None
        self.info_soup = None

        self._load_data()

    def _load_data(self):
        self.id = self.data['data']['id']
        self.title = self.data['data']['title']
        self.info = self.data['data']['info']
        self.table_name = self.data['table']
        if self.table_name == 'stang_pifu':
            self.cate_id = 0
        else:
            try:
                self.cate_id = self.data['data']['cate_id']
            # 当数据没有cate_id字段的时候，cate_id保持为None
            except KeyError:
                pass

    def _is_bid(self):
        if self.title is None or not self.title:
            return False
        if '中标' in self.title or self.cate_id == 2:
            return True
        else:
            return False

    def is_valid_bid(self):
        """是否为有效的中标数据(标题中包含‘中标’或者cate_id为2, 且info字段不为空)
        :return: True/False
        """
        if self.info is None or not self.info or len(self.get_info_text()) > 50000:  # 暂时解决下4924792 这样的情况，存在一个巨大表格
            return False
        if not self._is_bid():
            return False
        return True

    def get_the_data_to_redis(self):
        res = {}
        res['table_name'] = self.table_name
        keys = ['id', 'area_id', 'city_id', 'title', 'author', 'first_bid_company', 'first_bid_money',
                'invest_money', 'build_detail', 'build_owner', 'cate_id', 'label']
        for key in keys:
            try:
                value = self.data['data'][key]
                # 当值不存在的时候，设定为空字符串。例如在第一中标候选单位提取程序，会返回None, 这是时候将None转换为空字符串.
                if not value:
                    value = ''
            except KeyError:
                value = ''
            res[key] = value
        return res

    def get_the_data_to_es(self):
        res = {}
        res['table_name'] = self.table_name
        keys = ['id', 'area_id', 'city_id', 'title', 'author', 'first_bid_company', 'first_bid_money',
                'invest_money', 'summary', 'owner', 'add_time', 'cate_id', 'label', 'mode']
        for key in keys:
            try:
                value = self.data['data'][key]
                # 当值为None，设定为空字符串。例如在第一中标候选单位提取程序，会返回None, 这是时候将None转换为空字符串.
                if value is None:
                    value = ''
            except KeyError:
                value = ''
            res[key] = value
        return res

if __name__ == '__main__':
    from extraction import pattern
    from algorithm.bid_information import Information
    import json
    import os
    os.chdir(os.pardir)
    path = os.path.join('main_data', 'kafka_test_json.txt')
    with open(path) as f:
        data = f.read()
        print(type(data))
    dict_data = json.loads(data)
    test_data_obj = KafkaData(dict_data, pattern)
    print(test_data_obj.title)
    print(test_data_obj.get_info_sequence())
    print(test_data_obj.get_info_text())
    print(test_data_obj.get_info_soup())
    print(test_data_obj.get_tag_sequence())
    print(test_data_obj.is_valid())
    print(test_data_obj.is_valid_bid())
    print(test_data_obj.cate_id)
    ifm = Information(pattern)
    print(ifm.get_information(test_data_obj))
    print()