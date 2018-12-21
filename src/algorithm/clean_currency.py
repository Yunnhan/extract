from algorithm.clean_base import Clean
import re

class CleanCurrency(Clean):
    
    def get_target_list(self, amount_list,unit_list):
        """获得有效的target列表， (list, re.pattern, func) -> None/list
        :param target_list: 需要清理的target列表
        :param valid_pattern: 列表中每一个字符串必须满足pattern
        :param get_target_function: 对于不符合条件的字符串，清理出有效的target. 输入：字符串， 输出：该字符串中可能有效的target列表
        :return: 有效的target列表 None/list
        """
        if amount_list is None:
            return None
        if not amount_list:
            return None       
 
        #list 中的金额去重，并保持原顺序不变    
        amount_list = self.remove_duplicates(amount_list)
       #去除非法金额
        valid_list =[ self.is_valid(e) for e in amount_list ]
        a = [str(float(e)) for e in valid_list if e]
        
        if unit_list:   
            unit = list(set(unit_list))
#             print(unit)
            if len(unit)==1:
                if unit[0]=='元':   
                    valid_list=[e for e in a if float(e)>10000] 
#                     print('元',valid_list)
                    return self.filter(valid_list),unit
                else:
                    if '0.0' in a:
                        a.remove('0.0')     
                    return self.filter(a) ,unit                            
            else:
                print('单位不统一')
                print(unit,len(unit))
                return valid_list,unit
        else:
            print('没有单位')
            return valid_list,[]
         
    
    def is_valid(self, target):
        if target is None:
            return ''
        if not target:
            return ''
        if not isinstance(target, str):
            raise ValueError('clean, target 不是字符串')
        return  re.sub(r"[^\d\.]","",target) 
    
    def filter(self,res):         
        if len(res)>1:
            tmpp=res[0]   
            i=1 
            while i<len(res):
                if res[i][:2]==tmpp[:2] and res[i]!= tmpp :
                    res.remove(res[i])
                    i-=1
                else:
                    tmpp = res[i]
                i+=1
#             print('filtered:',res)
        return res
    