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
        for i in range(len(tokens)):
            tmp=' '
            if i+n>len(tokens):
                break
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
#         print(rough_text)
        tokens = self.compute_money.strip_noise(rough_text)
#         print('token',tokens)
        ngrams = self.n_grams(3,tokens)
#         print('ngram',ngrams)
        if not ngrams:
            print('None ngrams')
            return

       
        unit, text ,amount = self.compute_money.extract_candidate(ngrams)
#         print('origin',amount)
        if not unit or len(unit)!=1:
            if not unit:
                print('无单位')
            else :
                print(unit)
                print('单位不统一')
            return 
        amount = self.clean_currency.get_target_list(amount,unit)
#         print('proc',amount)
        if not amount:
            print('没有合法金额')
            return
#         print(amount)
        tmp = self.compute_money.get_currency(unit, text ,amount)
        if not tmp:
            print('没有结果')
            return
        return tmp
        
    
    
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
        print('text')
        res = self.text_search(bid_data_obj.get_info_soup())
       
        return res 
    
    
    def get_clean_text(self,info_soup):
        ''' 对soup sequence 进行进一步清理
        @info_soup:  Beautiful_soup 对象
        return list of string
        ''' 
        tokens=[]
        ls=[]
        for e in info_soup.strings:  
            e = re.sub('\n|\xa0','',e)
            if not e :
                continue
            string = ''
            for i in range(len(e)):
                if e[i]==' ':
                    j=i
                    while j>1 and e[j]==' ' :
                        j-=1
                    if not e[j].isdigit():
                        continue
                    else:
                        j=i
                        while j<len(e)-1 and e[j]==' ':
                            j+=1
                        if not e[j].isdigit():
                            continue
                string+=e[i]  
            ls.append(string)
               
        text= ' '.join(ls)
        text = re.sub("[+=！？~@#：……&*“”（）、【】]+|[\!_,$^*(+\"\'\);=><]|\xa0", "",text)  
        text = re.sub('： ',':',text)
        seg = re.split('\n|。|；|，|%',text)  # 切分
        for e in seg:
            if e and e!='\xa0' : #and len(e)>1
                e = re.sub('[\xa0\t\u3000]',' ',e)              
                if e:       
                    tokens.append(e) 
        info_seg = ' '.join(tokens)
        tmp = info_seg.split(' ')
        tokens = [e for e in tmp if e]            
#         text =[re.sub('[ \xa0\t\u3000、]','',e) for e in info_sequence ]
#         text = [e for e in text if e ]
        return tokens
        

