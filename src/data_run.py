from algorithm.create_df.read_data_lib.data_base import DataSQL
from algorithm.bid_information import Information
from extraction import pattern
from extraction.bid_data import BidData
from algorithm.analysis.see_html import See


class DataRun(object):
    """可以批量地进行数据的测试， 通过sql读取一部分id， 然后获取这部分id数据各个需要提取的字段的内容

    """
    def __init__(self):
        # 数据库
        self.db = DataSQL()
        # 初始化Information类，Information类为主要的提取程序，通过其方法get_information获取各个字段的内容
        # 位于algorithm.bid_information
        self.ifm = Information(pattern)
        # 辅助类，用以打开html， 将提取后的结果，与原先数据info内容合并为一个html，并使用浏览器打开，辅助检查提取算法的精度
        self.open_html = See()

    def _get_ids(self, sql):
        res = self.db.read_sql(sql)
        id_res = [ele[0] for ele in res]
        return id_res

    def main_with_open(self):
        # 读取批量测试数据的id
        sql = "SELECT id FROM stang_cbid WHERE cate_id = 2"
        # 读取上面sql，并将结果转化为id的列表
        ids = self._get_ids(sql)
        # 数据库
        cnn = DataSQL()
        for i in ids:
            try:
                # BidData数据类，实例化的时候，会从数据库中读取该id的数据，并将id, title, info, cate_id, table_name等信息信息存入
                # 该类的属性，并可以直接通过get_info_text等方法直接获取其去掉标签后的info字段内容
                bid_info = BidData(cnn, pattern, i, 'stang_cbid')
                # Information类为主要的提取程序，通过其方法get_information获取各个字段的内容， 传入的是BidData对象
                res = self.ifm.get_information(bid_info)
                # print(res, i)


                # 将提取后的结果插入数据库
                cnn.insert_data_with_table_name('stang_bid_extract_zid', bidid=i, first_bidcompany=str(res[0]),
                                                manager=str(res[1]), tablename='stang_cbid')

                # 打开html， 将提取后的结果，与原先数据info内容合并为一个html，并使用浏览器打开
                # self.open_html.open_html(i, 'stang_bid_new', extra_text=str(res))


                cnn.db.commit()
                # time.sleep(0.8)
            except Exception as e:
                print(e)



if __name__ == '__main__':
    dr = DataRun()
    print('start!')
    dr.main_with_open()