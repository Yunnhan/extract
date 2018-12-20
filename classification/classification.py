import pandas as pd
import jieba.posseg 
import pymysql
from bs4 import BeautifulSoup
import xlwt

class Classification:
    
    def __init__(self,pos = ['n', 'nz', 'v',  'vn', 'l'], special_word = ['EPC'],stopwords='stopwords.dat'):
        '''
        @sql: 数据库的完整查询语句
        @pos: 分词时想保留的词性。
        @special_word: 特殊的需要保留的词汇。例如英文，数字
        @stopwords: 自定义的停词文件。格式：一行一个词
        '''
       
        self.pos = pos
        self.special_word = special_word
        self.stopwords = stopwords
        self.dd =  {'防波堤': '航务工程', '生态湿地': '环境工程', '海绵': '海绵城市建设工程', '堆场': '航务工程', '水生态': '环境工程', 
                       '地基处理': '吹填及陆域形成工程', '蓝色海湾整治': '环境工程', '水工': '航务工程', '船闸': '航务工程', '围堤': '航务工程', 
                       '河道综合整治': '环境工程', '引水': '水利水电工程', '渠': '水利水电工程', '通道': '跨海通道工程', '堤防': '水利水电工程', 
                       '截污工程': '海绵城市建设工程', '河道整治': '环境工程', '挖泥': '疏浚工程', '围垦': '吹填及陆域形成工程', '疏浚': '疏浚工程', 
                       '填海': '吹填及陆域形成工程', '河道治理': '环境工程', '航道': '疏浚工程', '水系': '环境工程', '造林': '环境工程', 
                       '大堤': '水利水电工程', '海绵改造': '海绵城市建设工程', '水系连通': '环境工程', '下沉': '海绵城市建设工程', '滨水': '环境工程', 
                       '整治': '环境工程', '港区': '航务工程', '水利枢纽': '水利水电工程', '保护': '环境工程', '水利': '水利水电工程', '陆域形成': '吹填及陆域形成工程', 
                       '跨海': '跨海通道工程', '软基': '吹填及陆域形成工程', '清淤': '疏浚工程', '大桥': '跨海通道工程', '沉管': '跨海通道工程', '滨海': '环境工程',
                        '水厂': '环境工程', '水务': '环境工程', '管网': '海绵城市建设工程', '海底隧道': '跨海通道工程', '围填海': '吹填及陆域形成工程', '岛隧': '跨海通道工程',
                        '泊位': '航务工程', '排水': '海绵城市建设工程', '海堤': '航务工程', '港池': '疏浚工程', '治洪': '水利水电工程', '绿地': '海绵城市建设工程', 
                        '水环境': '环境工程', '海上风电': '海洋工程(海上风电)', '水体': '环境工程', '景观': '环境工程', '海绵城市': '海绵城市建设工程', 
                        '造地': '吹填及陆域形成工程', '航道整治': '航务工程', '渔港': '航务工程', '修复': '环境工程', '滨河': '环境工程', '港口码头': '航务工程', 
                        '生态环境': '环境工程', '污水处理': '环境工程', '透水': '海绵城市建设工程', '水质提升': '环境工程', '水资源': '环境工程', '生态': '环境工程', 
                        '防洪': '水利水电工程', '滑道': '航务工程', '维护疏浚': '疏浚工程', '船坞': '航务工程', '航电枢纽': '航务工程', '黑臭': '环境工程', '绿化': '环境工程'}    
        
    def convert_format(self,text):
        """去掉文本中的标签
        @text: 包含html的文本,string
        return: 不包含html的文本
        """
        return BeautifulSoup(text, 'lxml').text

    def get_tokens(self,text):
        """获取分词
        @text: 纯文本格式的string
        @pos: 选择保留的词性
        return: 分词后的结果，list形式
        """
        tokens=[]
        seg = jieba.posseg.cut(text)  # 分词
        for i in seg:
            if( i.word in self.special_word) or  i.word not in self.stopwords and i.flag in self.pos:  # 去停用词 + 词性筛选
                tokens.append(i.word)
        return ' '.join(tokens)
    
    
    def supplies_filter(self,df):
        '''去除物资类文档
        @df: 接收到的完整数据
        return: 从'采购'类文档中选出的非物资文档，dataframe格式 
        '''
        UNCORRELATED_TOPIC = '软件|硬件|系统|食堂|医院|学校|学院|小学|中学|农业|制作|信息|消防|数据|护理|计划生育|医疗|保安|配送|安保|广告|保洁|维保|学生|电信|财务|赛事|加油站|零星|照明|办公室'
        SUPPLIES_PATTERN = '设施|耗材|仪|器|安装|货|物资|备|配件|料|品|材|五金|工具|板|钢|盘|屏|盒|台|桌|椅|水管|铝|钉|塑|螺|泥|砖|锅|炉|卡|柜|机|车|苗|灯|书|牌|箱|电脑|废物|垃圾|蔬菜|水果|服装|衣|裤|家具|粮食|后勤|梯|本|询价|沙发|空调'
        
 
        if not df[df['title'].str.contains(UNCORRELATED_TOPIC)].empty:
            return pd.DataFrame()
        if not df[df['title'].str.contains(SUPPLIES_PATTERN)].empty:
            return pd.DataFrame()
        
        return  df
    
    def count_keywords(self,text_tokens,title_token,title_weight=10,info_weight=1): 
        ''' 统计文档内分属各类的关键词，取分数最大的为label
        @title_token: 标题的分词结果
        @text_tokens: 正文的的分词结果
        @title_weight: 出现在标题的关键词的权重，默认为10
        @info_weight: 出现在正文的关键词的权重，默认为1
        return: list：[分类，得分]
        '''
        count= {'疏浚工程': 0, '水利水电工程': 0, '环境工程': 0, '海洋工程(海上风电)': 0, '航务工程': 0, '吹填及陆域形成工程': 0, '跨海通道工程': 0, '海绵城市建设工程': 0}
        text_tokens = text_tokens.split(' ')
        title_token = title_token.split(' ')
        for ele in text_tokens:
            if ele in self.dd:
                count[self.dd[ele]]+=info_weight
        
        for ele in title_token :
            if ele in self.dd:
                count[self.dd[ele]]+=title_weight
        label = max(count, key=count.get)    
        if count[label]<3:
            return None
        return  [label,count[label]]
    
    def eight_category_classifier(self,df):
        '''按八个类别的关键词出现的词频，划分文档为八个类。丢弃无关文本
        @df: 非物资类文档的dafaframe
        return: dataframe
        '''
        df['title_token'] = df['title'].map(lambda x:self.get_tokens(x))
        df['info_token'] = df['info'].map(lambda x:self.get_tokens(self.convert_format(x)))        
        
        line = df.iloc[0]
        tmp = self.count_keywords(line.title_token,line.info_token)
        if tmp:
            label=tmp[0]
            score=tmp[1]
        else:
            return pd.DataFrame()
        
        df['label'] = label
        df['score'] = score
        
        return df
    
    def plan_or_bid(self,df):
        '''  区分拟建(0)/招标(1)/中标文件(2)
        @df:8分类好的文档，dataframe
        return : dataframe
        '''
        df.loc[df['title'].str.contains('招标'),'cate_id']=1
        df.loc[df['title'].str.contains('中标'),'cate_id']=2
        df.loc[df['title'].str.contains('批复|设计|监理|勘测|环境评测|规划|环评|勘察|竞争性|磋商|谈判|预审|可行性|EPC|总承包'),'cate_id']=0

        return df
           
    def generate_excel(self,df,filename):
        '''将df的内容写入excel
        @filename: excel文件名
        '''
        writebook = xlwt.Workbook(encoding='utf-8')
        sheet = writebook.add_sheet('data')  
        j=0 
        for i in range(df.shape[0]):        
            line = df.iloc[i]
            if not  line['label'] :
                continue
            sheet.write(j,0,str(line['id']))
            sheet.write(j,1,line['title'])
#             sheet.write(j,2,line['label'])
#             sheet.write(j,3,str(line['score']))
#             sheet.write(j,4,str(line['cate_id']))
            j+=1
        writebook.save(filename)
           
    def main(self,title,info,cate_id):
        '''对每一条数据，进行完整的分类过程
        @title: string格式
        @info: string格式
        @cate_id: string格式
        return： "cate_id", "label" 
        '''
        if not title or not info or not cate_id:
            return None,None
        tmp = [[title,info,int(cate_id)]]
        df = pd.DataFrame(tmp)
        df.rename(columns={0:'title',1:'info',2:'cate_id'},inplace=True)
        strip_supplies_data= self.supplies_filter(df)
        if not strip_supplies_data.empty:
            eight_category_data = self.eight_category_classifier(strip_supplies_data)
            if not eight_category_data.empty :
                plan_or_bid_data = self.plan_or_bid(eight_category_data)
                tmp = plan_or_bid_data.iloc[0]
                if tmp['cate_id']<3:
                    return tmp['cate_id'],tmp['label']
            
        return None,None
    
    def sql(self,arg):
        return "select title,info,cate_id from stang_cbid where id = %d " %(arg)

if __name__ == '__main__':
    c = Classification()
    db = pymysql.connect("192.168.1.252", "root", "123asd123asd", "suidaobig", charset='utf8')
    id=6170000
    while id<6180000:
        sql = c.sql(id)

        id+=1
        data = pd.read_sql(sql,db)
        if data.shape[0]==0:
            continue
        title = data.iloc[0]['title']
        info = data.iloc[0]['info']
        cate_id = data.iloc[0]['cate_id']
        tmp = c.main(title,info,cate_id)
        if tmp[0]==None:
            continue
        print(id,tmp)
        
#     df,last_id = c.main(sql)
#     while df.shape[0]<4000:
#         sql = c.sql(last_id)
#         tmp,last_id = c.main(sql)
#         df = df.append(tmp,ignore_index=True)
#         df = df.dropna(axis=0, how='any')
#         print(df.shape)
#         print(last_id)
# 
#     c.generate_excel(df,'test_final.xls')
    