from algorithm.create_df.read_data_lib.data_base import DataSQL
from algorithm.bid_information import Information
from extraction import pattern
from extraction.bid_data import BidData
import schedule
import time
import os

# 提取程序
ifm = Information(pattern)
# cbid表的最新id的储存路径
CBID_START_ID_PATH = os.path.join('main_data', 'cbid_start_id.txt')
# bid_new表的最新id的储存路径
BID_NEW_START_ID_PATH = os.path.join('main_data', 'bid_new_start_id.txt')


# 获取最新的id
def get_last_id(table_name, cnn):
    sql = "SELECT MAX(id) FROM {}".format(table_name)
    res = cnn.read_sql(sql)[0][0]
    return int(res)


# 获取cbid表上次定时任务的结束id， 本次任务的开始id
def load_cbid_start_id():
    with open(CBID_START_ID_PATH) as f:
        return int(f.read())


# 写入cbid表本次任务的结束id， 作为下次任务的开始id
def write_cbid_start_id(id):
    with open(CBID_START_ID_PATH, 'w') as f:
        f.write(str(id))


# 获取bid_new表上次定时任务的结束id， 本次任务的开始id
def load_bid_new_start_id():
    with open(BID_NEW_START_ID_PATH) as f:
        return int(f.read())


# 写入bid_new表本次任务的结束id， 作为下次任务的开始id
def write_bid_new_start_id(id):
    with open(BID_NEW_START_ID_PATH, 'w') as f:
        f.write(str(id))


# 开始提取cbid表中的数据
def begin_stang_cbid_job(cnn):
    # 加载本次任务的开始id（上次任务的结束id）
    start_id = load_cbid_start_id()
    # 获取数据库最新id，作为本次任务的结束id
    last_id = get_last_id('stang_cbid', cnn)
    # 当没有新数据的时候，结束本次任务
    if last_id == start_id:
        return None
    # 确保结束id 大于 开始id。 防止自己手动修改开始id的时候出错
    if last_id < start_id:
        raise AssertionError('last_id < start_id stang_bid_new')
    # 对于每一个id，进行字段的提取工作
    for each_id in range(start_id, last_id):
        print(each_id, 'cbid')
        # 读取数据，并返回一个bid_data对象，该对象包含了所有需要的数据
        bid_data = BidData(cnn, pattern, each_id, 'stang_cbid')
        # 判断info字段是否为空， 以及cate_id是否为2或者是否含有‘中标’关键词
        if not bid_data.is_valid_bid():
            continue
        # 提取第一中标单位和项目经理
        first_bid_company, manager = ifm.get_information(bid_data)
        # 将结果插入数据库
        cnn.insert_data_with_table_name('stang_bid_extract_zid', bidid=each_id, first_bidcompany=str(first_bid_company),
                                                manager=str(manager), tablename='stang_cbid')
        cnn.db.commit()
    # 储存本次任务的结束id， 作为下次任务的开始id
    write_cbid_start_id(last_id)


# 开始提取bid_new表中的数据
def begin_stang_bid_new_job(cnn):
    # 加载本次任务的开始id（上次任务的结束id）
    start_id = load_bid_new_start_id()
    # 获取数据库最新id，作为本次任务的结束id
    last_id = get_last_id('stang_bid_new', cnn)
    # 当没有新数据的时候，结束本次任务
    if last_id == start_id:
        return None
    # 确保结束id 大于 开始id。 防止自己手动修改开始id的时候出错
    if last_id < start_id:
        raise AssertionError('last_id < start_id stang_bid_new')
    # 对于每一个id，进行字段的提取工作
    for each_id in range(start_id, last_id):
        print(each_id, 'bid_new')
        # 读取数据，并返回一个bid_data对象，该对象包含了所有需要的数据
        bid_data = BidData(cnn, pattern, each_id, 'stang_bid_new')
        # 判断info字段是否为空， 以及cate_id是否为2或者是否含有‘中标’关键词
        if not bid_data.is_valid_bid():
            continue
        # 提取第一中标单位和项目经理
        first_bid_company, manager = ifm.get_information(bid_data)
        # 将结果插入数据库
        cnn.insert_data_with_table_name('stang_bid_extract_zid', bidid=each_id, first_bidcompany=str(first_bid_company),
                                                manager=str(manager), tablename='stang_bid_new')
        cnn.db.commit()
    # 储存本次任务的结束id， 作为下次任务的开始id
    write_bid_new_start_id(last_id)


# 每轮作业任务
def job():
    # 连接数据库
    cnn = DataSQL()
    # 提取bid_new表中的字段信息
    begin_stang_bid_new_job(cnn)
    # 提取cbid表中的字段信息
    begin_stang_cbid_job(cnn)
    # 关闭数据库
    cnn.db.close()


# 定时任务设置为每10分钟一轮
schedule.every(10).minutes.do(job)


# 开始定时任务
while True:
    schedule.run_pending()
    time.sleep(1)
