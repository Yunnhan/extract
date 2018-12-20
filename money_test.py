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
#     non=[70079, 70149, 70152, 70538, 70581, 70582,70583, 70862,70987,71275, 71341,  71435, 72054, 72132,72151,72334,72523,72720,72732,  72981, 73067, 73098, 73134, 73174, 73191,  73365, 73396,73541,73575, 73606,73666,73674, 73887,74078,74254,74423, 74817,74823, 74782,74952,75207, 75246,75277,75331, 75408, 75615, 75695,]
#     ids = [ 70155, 70360, 70387,  70963, 70984,  71008, 71013, 71307, 71471, 71590, 71616, 71717, 71805, 72082,  72193, 72244,  72379,  72569, 72577,  72927, 72986, 73009,73182, 73246, 73268, 73444,   73626,   73800, 73943, 74012,  74086, 74087, 74094, 74185, 74193,  74297, 74495, 74537,   74824, 74907, 74928, 74929,  75008,   75320, 75387,  75412, 75713, 75802, 75892]
    ids = [70079, 70149, 70152, 70155, 70360, 70387, 70538, 70581, 70582, 70583, 70862, 70963, 70984, 70987, 71008, 71013, 71275, 71307, 71341, 71435, 71471, 71590, 71616, 71717, 71805, 72054, 72082, 72132, 72151, 72193, 72244, 72334, 72379, 72523, 72569, 72577, 72720, 72732, 72927, 72981, 72986, 73009, 73067, 73098, 73134, 73174, 73182, 73191, 73246, 73268, 73365, 73396, 73444, 73541, 73575, 73606, 73626, 73666, 73674, 73800, 73887, 73943, 74012, 74078, 74086, 74087, 74094, 74185, 74193, 74254, 74297, 74423, 74495, 74537, 74782, 74817, 74823, 74824, 74907, 74928, 74929, 74952, 75008, 75207, 75246, 75277, 75320, 75331, 75387, 75408, 75412, 75615, 75695, 75713, 75802, 75892]
#     ids=[70583,70987,74423,74928]
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