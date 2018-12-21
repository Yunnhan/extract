from algorithm.feature_compute.compute import Computation
import pandas as pd
import re
import string
from sklearn.cluster import KMeans

class ComputationMoney(Computation):

    def __init__(self, pattern):
        super(ComputationMoney, self).__init__(pattern.KEY_PATTERN)
        self.pattern = pattern
        
    def strip_noise(self,tokens):
        '''去除明确的杂数据(对于金额数据)，为保持语义关系，用''占位
        return tokens (list)
        '''
#         UNCORRELATED = re.compile(u'\d{4}-\d{2}-\d{2}|( |电话|方式|号|率|路|传真|编|面积|地址|码|时间|[a-zA-Z])\D?\d+|万?( |天|家|年|月|日|时|分|面积|米|分|个|条款|线|号|公里|页|层|室|平方米|㎡|%)|(\d+-\d+)')            
        UNCORRELATED = re.compile(u'\d{4}-\d{2}-\d{2}|( |电话|方式|号|率|路|传真|编|面积|地址|码|时间|[a-zA-Z])\D?\d+|\d+万?( |天|家|年|月|日|时|分|面积|米|分|个|条款|线|号|公里|页|层|室|平方米|㎡|%)|(\d+-\d+)')        
        ALNUM = re.compile('[a-zA-Z0-9]')
        KEEP = re.compile(u'元|价|金额|总计|公司|￥|第(一|1)|元')
        ALLOWED =['天', '家', '年', '月', '日', '时', '分', '面积', '米', '分', '楼', '座', '室', '个', '条款', '线', '号', '公里', '页', '层', '室', '平方米', '㎡', '%']
        for i in range(len(tokens)):
            tmp =  UNCORRELATED.search(tokens[i])
            if tmp and KEEP.search(tokens[i]):
                while tmp:
                    s = tokens[i]
                    start = tmp.start()   
                    end = tmp.end()
                    k=1
                    for j in range(start,0,-1): 
                        if not ALNUM.search(s[j]) and not s[j] in string.punctuation and not s[j] in ALLOWED :
                            k=j
                            break
                    pre = s[:k]
                    k=len(s)-1
                    if end ==len(s):
                        s = pre
                    else:
                        for j in range(end,len(s)):  
                            if not ALNUM.search(s[j]) and not s[j] in string.punctuation and not s[j] in ALLOWED :
                                k=j-1
                                break   
                    tokens[i]=pre+s[k+1:] 
                    tmp = UNCORRELATED.search( pre+s[k+1:] )                      
            elif tmp:
                tokens[i]=''
                
            if tokens[i].startswith('0') :#or len(tokens[i])==1
                tokens[i]=''
        return tokens 

    def check_locs_values(self,df,key_locs):
        '''提取key_locs对应单元格中的金额数据
                    对于一个单元格内太多信息的情况，跳过  todo
        return
        '''
        NUMBER = re.compile('\d+\.?\d+')
        UNIT = re.compile('万?元')
        values= self.get_cell_from_locs(df,key_locs)
        if not values:
            return None,None
        res=[]
        units = []
        for ele in values:
            tmp = NUMBER.findall(ele)
            unit = UNIT.findall(ele)
            if unit :
                units.extend(unit)
            if not tmp :
                continue
            if len(tmp)>1 :
#                 if len(values)==1:
#                     print(ele)
#                     ele = self.strip_noise([ele])[0]
#                     if ele and ele!='':
#                         print(ele)
#                         tmp = NUMBER.findall(ele)
#                 else:
                continue
            res.extend(tmp)
        
            
        return res,units
     
    def get_intersection_point(self,locs1,locs2):
        '''生成locs1 和 locs2 两个df中坐标的交点坐标
                    存在df中返回
        '''
        ls =[]
        if locs1 is None:
            return None
        if locs2 is None:
            return None
        for i in range(locs1.shape[0]):
            line1 = locs1.iloc[i]
            x1 = line1['行号']
            y1 = line1['列号']
            for j in range(locs2.shape[0]):
                line2 = locs2.iloc[j]
                x2 = line2['行号']
                y2 = line2['列号']
                ls.append([x1,y2])
                ls.append([x2,y1])
        ls_df = pd.DataFrame(ls,columns=['行号','列号'])
        return ls_df     
            
    def get_surrounding_cell_text_from_one_df(self, df, key_pattern,auxiliary_key):
        """ 从一个df中获取关键词‘右方’或者‘下方’的内容（cell里面的全部内容） (df, re.pattern) -> None/list
        :param df: 原始df
        :return: None/list
        """
        if df is None:  # 检查df
            print('空df')
            return None
        if df.shape[0] == 0:  # 检查是否为空df
            print('空df')
            return None
        auxiliary_key_locs = self.get_location(df, auxiliary_key) 
        key_locs = self.get_location(df, key_pattern)  # 获取关键词位置
        if key_locs is None or  key_locs.shape[0] == 0:  # 检查关键词位置是否有效
            print('no keyword')
            return None
        if auxiliary_key_locs is not None and auxiliary_key_locs.shape[0]>0:
            intersection = self.get_intersection_point(key_locs,auxiliary_key_locs)
            tmp = self.check_locs_values(df,intersection)
            if tmp[0]:
                print('交叉')
                return tmp
        res,units = self.check_locs_values(df,key_locs)
        if res:  
            print('key')
            return res,units
    
        search_direction = self.get_df_type_pro(df, is_weak=True)  # 获取df的类型， 是‘横向查找’还是‘纵向查找’。
        if search_direction == '纵向查找':
            search_loc = self.get_next_vertical_cell(key_locs, df.shape)  # 获取关键词下方的位置
            res_tem,unit_tmp = self.check_locs_values(df, search_loc)  # 获取关键词位置下方的内容
            if res_tem:
                print('纵向')
                res += res_tem  # 存入结果
            if unit_tmp:
                units+=unit_tmp
        elif search_direction == '横向查找':     
            search_loc = self.get_next_horizontal_cell(key_locs, df.shape)  # 获取关键词右方的位置
            res_tem,unit_tmp = self.check_locs_values(df, search_loc)  # 获取关键词位置右方的内容
            if res_tem:
                print('横向')
                res += res_tem  # 存入结果
            if unit_tmp:
                units+=unit_tmp
        else:  # 当无法判定df为‘横向查找’还是‘纵向查找’的时候，同时获取关键词右方和下方的内容
            print('both')
            search_loc_ver = self.get_next_vertical_cell(key_locs, df.shape)  # 获取关键词下方的位置
            res_tem_ver,unit_tmp_ver = self.check_locs_values(df, search_loc_ver)  # 获取关键词位置下方的内容
            search_hor = self.get_next_horizontal_cell(key_locs, df.shape)  # 获取关键词右方的位置
            res_tem_hor,unit_tmp_hor = self.check_locs_values(df, search_hor)  # 获取关键词位置右方的内容
            if res_tem_ver:
                res += res_tem_ver  # 存入结果
            if unit_tmp_ver:
                units+=unit_tmp_ver
            if res_tem_hor:
                res = res + res_tem_hor  # 存入结果
            if unit_tmp_hor:
                units+=unit_tmp_hor
        if not res:  # 检查
            return None
        res = list(filter(lambda text: len(str(text)), res))  # 去除空字符串的结果（exist some better ways）
        return res,units
    
    
    def get_surrounding_cell_text_from_dfs(self, dfs, key_pattern,auxiliary_key):
        """获得关键词（key_pattern）附近的单元格的内容 (pd.DataFrame, re.pattern) -> None/list
        会判断每一个df的类型，该横向查找target，还是纵向查找target， 或者是横向纵向同时查找target.
        :param dfs: list of pandas dataframe
        :param key_pattern: 关键词
        :return: 关键词附近的单元格内容
        """
        if dfs is None:  # 检查dfs是否为None
            print('空dfs')
            return None
        res = []  # 结果暂存
        units=[]
        # 遍历df，获取关键词的‘后一个’位置 或者 ‘下一个’位置 的所有文本信息
        for df in dfs:
            tmp = self.get_surrounding_cell_text_from_one_df(df, key_pattern,auxiliary_key)  # 遍历每一个df， 获取关键词‘下右’方文本
            if tmp :   # 如果存在，那么存入res
                res += tmp[0]
                units +=tmp[1]
        if not res:
            return None
        return res,units
    
    def extract_candidate(self,ngrams):      
        '''粗提取，提取所有可能的金额候选及对应文本。并 对不合理的数字进行过滤
        @ngrams：list of n-gram tokens
        return：
        unit:单位 （当前只处理有且仅有一个单位的文本） todo
        amount: 中标金额候选数据
        text：候选数据对应文本
        '''
        UNIT = re.compile(u'万*元')
        NUMBER = re.compile(u'((万*元|总计|价|金额|￥)*:?\d+(\.\d+)?)')
        text=[]
        amount = []
        unit=set()
        for e in ngrams:
            unit.update(UNIT.findall(e))
            a = NUMBER.findall(e)
            if a:
                for x in a:
                    tmp = x[0] 
                    if len(tmp)==1 or tmp.startswith('0') or tmp.count('.')>1:
                        continue
                    amount.append(tmp)
                text.append(e)
        unit = list(unit)
        if not amount:
            print('没有金额')
            return None,None,None
        #         amount= self.validate_candidate(unit, amount)
        return unit, text ,amount

 
    def pattern_search(self,pattern,unit,text,amount):
        '''
        @pattern：用于匹配的正则
        @unit： 提取的单位信息
        @text：初步提取出来的候选信息
        @amoutn: 候选金额
                    在每一个匹配到pattern的文本段落中提取数字信息，并验证是否在候选金额当中。
        return： list of money , list of unit    or   None      
        '''
        extract = re.compile('\d+\.?\d+')
        money=[]
        exclude = re.compile(pattern)
        for i in range(len(text)):
            if exclude.search(text[i]):      
                tmp_ls = extract.findall(text[i])
                if not tmp_ls:
                    continue
                tmp=[e for e in tmp_ls if str(float(e)) in amount]  
                if tmp and tmp[0] not in money:
                    money.append(tmp[0] )
        if money :   
            return money,unit
        return None
     
    def pattern_match_then_search(self,pattern,unit,text,amount):
        '''  由于n-grams，考虑词序，先用match匹配 ，以避免杂数据
                    只取第一个匹配结果
        return： list of money , list of unit  
        '''
        extract = re.compile('\d+\.?\d+')
        exclude  = re.compile(pattern)
        money=[]
        candidate=[]
        for i in range(len(text)):        
            if exclude.match(text[i]):
                tmp_ls = extract.findall(text[i])     
                if not tmp_ls:
                    continue
                tmp=[e for e in tmp_ls if e!='' and str(float(e)) in amount]   
                if tmp:
                    money = tmp[0]
                    break               
            elif not candidate:
                if exclude.search(text[i]):
                    tmp_ls = extract.findall(text[i])
                    if not tmp_ls:
                        continue
                    tmp=[e for e in tmp_ls if str(float(e)) in amount]  
                    if tmp:
                        candidate=tmp[0]
        if money:
            return money,unit
        if candidate:   
            return candidate,unit
        return None
    
    def k_means(self,amount):
        '''利用聚类区分两个标段各相等数据的情况
        @amount： list of money
        return： true/false
        '''
        print('kmeans')
        n = len(amount)//2
        x = [[float(e)] for e in amount]
        kmeans = KMeans(n_clusters=2,random_state = 0,max_iter=10).fit(x)
        labels = kmeans.labels_
        y1 = [1]*n + [0]*n
        y2 = [0]*n + [1]*n
        if (labels ==y1).all() or (labels ==y2).all():
            return True
        return False
    
    
    def get_currency(self,unit,text,amount):
        ''' 
        1.唯一合法金额时，直接返回
        2.有第一修饰，多个中标候选人的情况，对应多个中标金额，全部保留
        3.否则，按报价等关键词取第一个匹配
        4.没有明确关键词时，用公司定位，取出多个数据时，用kmeans做筛选
        return： list of money , list of unit  
        '''
        #唯一数字情况
        if len(amount)==1:
            return [amount[0]],[unit[0]]
        # 多个中标候选人的情况，对应多个中标金额
        key_pattern ='''第(1|一)名|第(一|1)中标|第一预中标候选(人|单位|名称)|中标供应商|(中标|成交)供应商(名称|单位)?|中标社会资本|
                                                        成交社会资本(名称|方|单位名称)|拟中标(单位|人|供应商)|第一候选(人|单位|名称|成交人)|(推荐)中标候选人|首选中标候选人'''
        res = self.pattern_search(key_pattern,unit,text,amount)
        if res:
            return res

        # ---用投标价格取第一个价格
        res = self.pattern_match_then_search('投标.*价|投标金额|中标.*(金额|价)|中标值|成交金额|总中标金额|最终报价|成交价|￥',unit,text,amount)
        if res:
            return res
          
        res = self.pattern_search('公司',unit,text,amount) 
        if res:
            if len(res[0])>2 and len(res[0])%2==0 and self.k_means(res[0]):
                tmp = [res[0][0],res[0][len(res[0])//2]]
                return tmp,res[1]
            return res
        
        return 
    
    
        