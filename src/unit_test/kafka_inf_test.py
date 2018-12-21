from algorithm.kafka_information import KafkaInformation
from extraction import pattern
import os
from extraction.bid_data import BidData
from algorithm.create_df.read_data_lib.data_base import DataSQL
os.chdir(os.pardir)
k = KafkaInformation(pattern)
ids = [12594272, 12409355, 12101817, 12101817, 12594272, 12659086, 12184825, 12191163, 12414575]
cnn = DataSQL()
for id in ids:
    data = BidData(cnn, pattern, id, 'stang_bid_new')
    print(k.get_first_company_zj(data))

cbid_ids = [6492022]
for id in cbid_ids:
    data = BidData(cnn, pattern, id, 'stang_cbid')
    print(k.get_first_company(data))