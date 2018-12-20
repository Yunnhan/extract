from algorithm.clean_base import Clean
import re

class CleanCurrency(Clean):
    
#     , valid_pattern, get_target_function
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
        valid_list =[ e for e in amount_list if self.is_valid(e)]
        a = [str(float(e)) for e in valid_list]
        
        if unit_list:   
            unit = list(set(unit_list))
            
            if len(unit)==1:
                if unit[0]=='元':   
                    valid_list=[e for e in a if float(e)>10000]   
                else:
                    if '0.0' in a:
                        a.remove('0.0')
                    tmp = [float(e) for e in a ]
                    max_amount = max(tmp)
                    min_amount = min(tmp)
                    if max_amount - min_amount>100:
                        a.remove(str(min_amount))
                    valid_list=a
                             
            else:
                print('单位不统一')
                print(unit,len(unit))
                return valid_list
        else:
            print('没有单位')
            return valid_list
         
        return valid_list
    
    def is_valid(self, target):
        if target is None:
            return False
        if not target:
            return False
        if not isinstance(target, str):
            raise ValueError('clean, target 不是字符串')
        return  re.sub(r"[^\d\.]","",target) 
    
    
    