import json
from extraction.bid_data import BidData


class KafkaData(BidData):

    def __init__(self, data, pattern):
        if not isinstance(data, dict):
            raise ValueError('data is not a dict')
        self.id = data['data']['id']
        self.title = data['data']['title']
        self.info = data['data']['info']
        self.cate_id = data['data']['cate_id']
        self.table_name = data['table']
        self.pattern = pattern
        self.info_text = None
        self.info_soup = None
        self.info_sequence = None
        self.tags_sequence = None
        self.dfs = None

if __name__ == '__main__':
    from extraction import pattern
    from algorithm.bid_information import Information
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



