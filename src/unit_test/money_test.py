from algorithm.money_infomation import MoneyInformation
from extraction import pattern
from algorithm.create_df.read_data_lib.data_base import DataSQL
from extraction.bid_data import BidData

class MoneyTest(object):
    """金额测试
    """
    def __init__(self):
        self.cnn = DataSQL()  # 数据库
        self.ifm = MoneyInformation(pattern)  # 提取money主程序

    def test_one(self, id, table_name):
        data = BidData(self.cnn, pattern, id, table_name)  # bid_data_object
        res = self.ifm.get_first_bidmoney(data)  # 获取金额，
        return res


if __name__ == '__main__':
    m = MoneyTest()
    ids = [10002694]
    for id in ids:
        print(m.test_one(id, 'stang_bid_new'))