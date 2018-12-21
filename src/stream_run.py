from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from extraction import pattern
from algorithm.kafka_information import KafkaInformation
from extraction.kafka_data import KafkaData
from classification.classification import Classification
import json
import time
from redis import  StrictRedis
import pandas as pd


class Stream(object):

    def __init__(self, pattern):
        # kafka 配置
        self.consumer = KafkaConsumer("maxwell", bootstrap_servers=['192.168.1.208:9092','192.168.1.212:9092','192.168.1.211:9092'])
        # es 配置
        self.es = Elasticsearch([{'host': '192.168.1.252', 'port': 9202}, {'host': '192.168.1.193', 'port': 9202},
                                 {'host': '192.168.1.194', 'port': 9202}])
        # 一些正则表达式
        self.pattern = pattern
        # 分类程序，区别（物资/非物资；港航/非港航；拟建/招标/中标）
        self.clf = Classification()
        # 字段提取程序，提取第一中标候选人，中标金额，投资金额，业主，项目概况等信息
        self.ifm = KafkaInformation(pattern)
        # redis 配置， 暂时没有使用
        self.redis = StrictRedis('192.168.1.60', port=6379, db=7, password='foobared')

    #  主程序， 从kalfka读取数据，处理后直接存入es
    def consume(self):
        # 持续从kafka读取数据
        for msg in self.consumer:
            # 解析读取后的数据
            key = json.loads(msg.key.decode())
            value = msg.value.decode()
            # 如果不是stang_bid_new, stang_cbid, stang_pifu这三个表的数据，直接丢弃
            if not self.is_valid_table_name(key['table']):
                pass
            else:
                try:
                    # 分类以及字段提取持续
                    res = self.job_run(value)
                    # 仅仅将需要的数据存入es
                    if res:
                        # 存入es的doc的id为 数据表名_id
                        doc_id = str(res['table_name']) + '_' + str(res['id'])
                        self.es.index(index='ccccltd', doc_type='shipping', body=res, id=doc_id)
                except Exception as e:
                    print(e)

    # 判断是否是 stang_bid_new, stang_cbid 和 stang_pifu表中的数据
    def is_valid_table_name(self, database_name):
        if database_name is None:
            return None
        if not isinstance(database_name, str):
            raise ValueError('data base table name is not valid')
        if database_name in self.pattern.valid_database:
            return True
        else:
            return False

    # 将从kafka读取到的数据，转换为kafka_data对象
    def get_data(self, value):
        try:
            dict_value = json.loads(value)
            kafka_data = KafkaData(dict_value, self.pattern)
            return kafka_data
        except Exception as e:
            print('Get kafka data object data wrong', e)

    # 对于每一条数据的处理程序
    def job_run(self, values):
        # 获取kafka_data对象
        kafka_data = self.get_data(values)
        # 获取拟建\招标\中标以及所属八类别， 当不是需要的数据的时候会返回None, None(例如非八大类，非拟建招中标， 或者是物资的数据)
        cate_id, label = self.clf.main(kafka_data.title, kafka_data.get_info_text(), kafka_data.cate_id)
        # 当为有效的数据时候
        if cate_id is not None:
            # 所属类别
            kafka_data.cate_id = cate_id
            # 字段提取
            first_bid_company, first_bid_money, invest_money, summary, owner = self.ifm.get_information(kafka_data)
            # 所属类别
            kafka_data.data['data']['cate_id'] = int(cate_id)
            # 八大类标记
            kafka_data.data['data']['label'] = label
            # 第一中标单位
            kafka_data.data['data']['first_bid_company'] = first_bid_company
            # 中标金额
            kafka_data.data['data']['first_bid_money'] = first_bid_money
            # 投资额
            kafka_data.data['data']['invest_money'] = invest_money
            # 项目概况
            kafka_data.data['data']['summary'] = summary
            # 业主
            kafka_data.data['data']['owner'] = owner
            # es的入库时间， 毫秒级
            kafka_data.data['data']['add_time'] = int(round(time.time() * 1000))
            # pubtime, 毫秒级别
            kafka_data.data['data']['pub_time'] = int(str(str(pd.Timestamp(kafka_data.data['data']['pubtime']).value)[:13]))
            # 获取需要插入es的数
            res = kafka_data.get_the_data_to_es()
            # print(res, '现在打印的是res')
            return res
        # 当不是需要的数据的时候返回None(例如非八大类，非拟建招中标， 或者是物资的数据)
        else:
            return None


if __name__ == '__main__':
    s = Stream(pattern)
    print('now star')
    s.consume()


