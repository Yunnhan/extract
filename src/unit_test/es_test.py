from elasticsearch import Elasticsearch
import pandas
import re
import json

class EsData():

    def __init__(self):
        self.es_new = Elasticsearch([{'host': '192.168.1.193', 'port': 9202}])

    def get_query(self, id):
        query = {
          "table_name": "stang_bid_new",
          "id": 11223344,
          "area_id": 5,
          "city_id": 5,
          "cate_id": 10,
          "title": "不给太平洋加盖遮阳棚项目",
          "author": "隧唐科技有限公司",
          "invest_money": 1500000,
          "mode": "公司独资",
          "summary": "人类的福祉",
          "owner": "科技有限公司",
          "first_bid_company": "中铁一百局集团有限公司",
          "first_bid_money": 15000,
          "add_time": "1544672882",
          "label": "克格勃"
        }


        return query


if __name__  == "__main__":
    e = EsData()
    # e.es_search(22)
    query = '''{
            "query": {
            "match_all": {}
            }
            }'''
    # print(e.es.get(index='stang_bid', doc_type='stang_cbid', id=54987))
    # print(e.es_old.search(index='stang_bid', body=query))
    # query = e.get_query(2)
    print(e.es_new.search(index='ccccltd', body=query))
    # e.es_new.index(index='ccccltd', doc_type='shipping', body=query)
    print('完成！')
