from algorithm.money_infomation import MoneyInformation
from extraction import pattern
from algorithm.create_df.read_data_lib.data_base import DataSQL
from extraction.bid_data import BidData
from algorithm.clean_currency import CleanCurrency

import re

class MoneyTest(object):
    """金额测试
    """
    def __init__(self):
        self.cnn = DataSQL()  # 数据库
        self.ifm = MoneyInformation(pattern)  # 提取money主程序
        self.clean_currency = CleanCurrency(pattern)
        

    def test_one(self, id, table_name):
        data = BidData(self.cnn, pattern, id, table_name)  # bid_data_object
        candidates = self.ifm.get_first_bidmoney(data)  # 获取金额，
        return candidates
#         if not candidates:
#             return None 
#         return res,candidates[1]
    
#         return self.
if __name__ == '__main__':
    m = MoneyTest()
    ids = [100018, 100019, 100055, 100057, 100072, 100102, 100106, 100187, 100199, 100217, 100265, 100291, 100341, 100374, 100409, 100443, 100451, 100499, 100500, 100502, 100523, 100614, 100638, 100660, 100680, 100715, 100781, 100782, 100783, 100784, 100785, 100787, 100788, 100899, 100947, 100953, 100988, 101082, 101090, 101104, 101128, 101130, 101131, 101134, 101135, 101136, 101137, 101160, 101166, 101169, 101256, 101262, 101263, 101264, 101265, 101266, 101268, 101342, 101373, 101539, 101610, 101616, 101619, 101626, 101632, 101660, 101664, 101669, 101683, 101724, 101731, 101764, 101769, 101808, 101971, 101984, 102004, 102026, 102042, 102043, 102060, 102079, 102087, 102114, 102115, 102117, 102118, 102161, 102184, 102206, 102227, 102231, 102284, 102307, 102331, 102366, 102375, 102396, 102397, 102443, 102451, 102474, 102495]
#     ids=[72193,74537,75331]
#     ids = [70987]
    
    count = 0
    for id in ids:
        print(id)
        a = m.test_one(id, 'stang_bid_new')
#         a = m.clean_text(id, 'stang_bid_new')
        if a :
            print('===================')
            count+=1
            print(a[0],end='')
            if len(set(a[1]))==1:       
                print(a[1][0])
            else:
                print('单位不统一',a[1])
        else :
            print('none')
        print('--------------')
    print('total:' ,len(ids),'get:',count)