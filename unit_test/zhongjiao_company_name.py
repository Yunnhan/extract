import pandas as pd
import os
import re

os.chdir(os.pardir)

path = os.path.join('algorithm\sub_data', 'zhongjiao_company_name.xlsx')

zj_company_name_df = pd.read_excel(path)
zj_company_name_df['name_without_zj'] = zj_company_name_df['company_name'].apply(lambda x: x[2:])
zj_company_name_df.to_excel(path)
print(zj_company_name_df.head())