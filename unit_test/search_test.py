from algorithm.sequence_search import SequenceSearch


class Res:

    def __init__(self, pattern):
        self.sqs = SequenceSearch(pattern)

    def main(self, id, table_name):
        return self.sqs.get_target(id, table_name)


if __name__ == '__main__':
    import os
    from extraction import pattern
    from algorithm.create_df.read_data_lib.data_base import DataSQL
    from extraction.bid_data import BidData
    from algorithm.nlp_algorithm.ltp_algorithm import Ner
    # 无法进行测试，模型不能够成功加载
    ner = Ner()
    os.chdir(os.pardir)
    os.chdir(os.pardir)
    df_pre = Res(pattern)
    cnn = DataSQL()
    bid_data = BidData(cnn, pattern, 23540, 'stang_bid_new')
    ids = [8160283, 10115126, 10080145, 9493171, 10125682, 10125613]
    for i in ids:
        data = BidData(cnn, pattern, i, 'stang_bid_new')
        print(df_pre.sqs.get_target(data.get_tag_sequence(), pattern.FIRST_KEY_PATTERN, ner.get_institution_from_string))