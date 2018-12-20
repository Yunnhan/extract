import pandas as pd
import os
from algorithm.analysis.see_html import See
from algorithm.kafka_information import KafkaInformation
from extraction import pattern
from algorithm.create_df.read_data_lib.data_base import DataSQL
from extraction.bid_data import BidData


def load_data():
    path = os.path.join(r'unit_test\test_data', 'test_final.xls')
    data = pd.read_excel(path)
    data.columns = ['id', 'title']
    return data


def open_nijian_data(df, ifm, cnn, open):
    # open.open_html()
    df = df.sample(frac=1)
    for id in df['id'].head(30):
        # open.open_html(id, 'stang_bid_new')
        data = BidData(cnn, pattern, id, 'stang_bid_new')
        res = ifm.get_owner(data)
        open.open_html(id, 'stang_bid_new', extra_text=str(res))


def open_nijian_data_summary(df, ifm, cnn, open):
    # open.open_html()
    df = df.sample(frac=1)
    for id in df['id'].head(80):
        # open.open_html(id, 'stang_bid_new')
        data = BidData(cnn, pattern, id, 'stang_bid_new')
        res = ifm.get_summary(data)
        open.open_html(id, 'stang_bid_new', extra_text=str(res))


def test_main(id, ifm, cnn, open):
    data = BidData(cnn, pattern, id, 'stang_bid_new')
    res = ifm.get_owner(data)
    return res


def test_summary_main(id, ifm, cnn, open):
    data = BidData(cnn, pattern, id, 'stang_bid_new')
    res = ifm.get_summary(data)
    return res


if __name__ == '__main__':
    import os
    os.chdir(os.pardir)
    open = See()
    cnn = DataSQL()
    df = load_data()
    ifm = KafkaInformation(pattern)
    # 业主测试代码
    # open_nijian_data(df, ifm, cnn, open)
    # 项目概况测试代码
    # open_nijian_data_summary(df, ifm, cnn, open)

    # 精确测试代码，通过字符串，以及id
    # print(ifm.ner.get_institution_from_string('建设单位:湖北省宜昌市水文水资源勘测局'))
    owner_string_test = ['招 标 人：京山县钱场镇卫生院 ',
                         '采购人：卫辉市水利局',
                         '采购单位：长春市绿园区园林管理处',
                         '招 标 人：尉氏县城市管理局',
                         '招 标 人：临江市鸭绿江堤防工程管理处',
                         '采购单位：长春市绿园区园林管理处', '采购人：长安区城市管理局', '建设单位:湖北省宜昌市水文水资源勘测局']
    # for string in owner_string_test:
    #     print(ifm.ner.get_owner_from_string_checked_len(string))
    test_ids = [12227511, 12151179, 9224681, 9245252, 9423004, 9296065, 9175416, 9296065, 9259112, 9224605, 9217744, 9195213, 9469427, 9469495, 9178434,
                9179299, 9181667, 9182848]
    for id in test_ids:
        print(id, test_summary_main(id, ifm, cnn, open), end='\n-------------------------------------------------------\n')