from algorithm.bid_information import Information
from algorithm.feature_compute.compute_money import ComputationMoney
from algorithm.clean_currency import CleanCurrency
from extraction import pattern
from algorithm.tag_search import TagSearch
import re

class MoneyInformation(Information):
    def __init__(self, pattern):
        super(MoneyInformation, self).__init__(pattern)  # 继承的information类
        self.compute_money = ComputationMoney(pattern)  # 作用于金额，金额类别
        self.clean_currency=CleanCurrency(pattern)
        self.tag_search = TagSearch()
    
    def n_grams(self,n,tokens):   
        '''为取得语义关联信息，将ls中的连续n个token结合在一起
        @n：n-grams
        @ls：切分好的token，list
        return list
        '''
        ngrams =[]
        for i in range(len(tokens)-n):
            tmp=' '
            for j in range(i,i+n):
                if not tokens[j]:
                    continue
                if tmp[-1].isdigit() and tokens[j][0].isdigit():
                    tmp+=' '+tokens[j]
                else:
                    tmp+=tokens[j]
            tmp = re.sub('中标',' 中标',tmp)
            tmp = re.sub('第[一|1] 中标',' 第一中标',tmp)
            tmp = re.sub('第[^一|1] ',' 第二',tmp)
            ls = [e for e in tmp.split(' ') if e and e not in ngrams ]
            ngrams.extend(ls)
        return list(ngrams)
    
    def text_search(self,info_sequence):
        '''读入文档进行字段提取
        '''   
        rough_text = self.get_clean_text(info_sequence)
        tokens = self.compute_money.strip_noise(rough_text)
        ngrams = self.n_grams(3,tokens)
        if not ngrams:
            print('None ngrams')
            return
#         print(tokens)
#         print('ngram',ngrams)
        unit, text ,amount = self.compute_money.extract_candidate(ngrams)
        amount = self.clean_currency.get_target_list(amount,unit)
        if not amount:
            print('没有合法金额')
            return
#         print(amount)
        tmp = self.compute_money.get_currency(unit, text ,amount)
        
    
    
    # 主要实现方法
    def get_first_bidmoney(self, bid_data_obj):
        if not bid_data_obj.is_valid_bid():
            return None, None
        res=[]
        dfs = self.df_pre.get_all_valid_pandas_df(bid_data_obj)
        key_pattern = pattern.PRICE_PATTERN
        auxiliary_key =re.compile('第[一1]')
        tmp = self.compute_money.get_surrounding_cell_text_from_dfs(dfs, key_pattern,auxiliary_key)
        if tmp:
            res= self.clean_currency.get_target_list(tmp[0],tmp[1])
            return res,tmp[1]
        res = self.text_search(bid_data_obj.get_info_sequence())
       
        
        return res 
    
    
    def get_clean_text(self,info_sequence):
        ''' 对soup sequence 进行进一步清理
        @info_soup:  Beautiful_soup 对象
        return list of string
        ''' 
        print(info_sequence)
        text = [re.sub('[ \xa0\t\u3000、]','',e) for e in info_sequence ]
        text = [e for e in text if e ]
        return text
        

