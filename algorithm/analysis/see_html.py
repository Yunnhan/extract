import os
# import pandas as pd
import pymysql

class See:

    def __init__(self):
        db = pymysql.connect("192.168.1.252", "root", "123asd123asd", "suidaobig", charset='utf8')
        self.cursor = db.cursor()

    # 从数据库读取数据
    def read_data_from_database(self, table_name, db_id=None, title=None, column_name=None, value=None, random_nums=None, sql_text=None):
        if sql_text and random_nums:
            try:
                self.cursor.execute(sql_text)
                result = self.cursor.fetchall()[:random_nums]
            except:
                return None
            return result

        if column_name and value and random_nums:
            sql = "SELECT title, info, id FROM {0} WHERE {1} = {3}{2}{3} ORDER BY rand() LIMIT 100"\
                  .format(table_name, column_name, value, "'")
            try:
                self.cursor.execute(sql)
                result =self.cursor.fetchall()[:random_nums]
            except:
                return None
            return result

        if column_name and value:
            sql = "SELECT title, info, id FROM {} WHERE {} LIKE '%{}%'".format(table_name, column_name, value)
            try:
                self.cursor.execute(sql)
                result = self.cursor.fetchone()
            except:
                return None
            return result

        if db_id:
            db_id = str(db_id)
            # SQL = 'SELECT info, title FROM stang_bid WHERE id = ' + db_id
            sql = 'SELECT title, info, id FROM {} WHERE id = {}'.format(table_name, db_id)
            try:
                self.cursor.execute(sql)
                result = self.cursor.fetchone()
            except:
                # print('ERROR: unable to find the data')
                return None
            return result

        if title:
            sql = 'SELECT title, info, id FROM {0} WHERE title = {2}{1}{2}'.format(table_name, title, "'")
            try:
                self.cursor.execute(sql)
                result = self.cursor.fetchone()
            except:
                # print('ERROR: unable to find the data')
                return None
            return result



    def open_html(self, db_id, table_name, title=None, column_name=None, value=None, random_nums=None, sql_text=None,
                  extra_text=''):
        if random_nums:
            random_nums = int(random_nums)
        result = self.read_data_from_database(table_name, db_id, title, column_name, value, random_nums, sql_text)
        if result:
            if isinstance(result[0], tuple):
                for i in range(len(result)):
                    info = extra_text + result[i][1]
                    self.__open_one_html(result[i][2], info)
            else:
                info = str(extra_text) + result[1]
                self.__open_one_html(result[2], info)

    def __open_one_html(self, id, info):
        name = 'htmls\{}.html'.format(str(id))
        if not os.path.exists('htmls'):
            os.mkdir('htmls')
        try:
            with open(name, 'w', errors='ignore') as f:
                f.write(info)
        except:
            pass
        os.system(name)


if __name__ == '__main__':
    S = See()
    # S.open_html(8879040)
    # S.open_html('stang_leader_activity', None, '孟凤朝庄尚标拜会湖南省委书记杜家毫')
    # S.open_html(7281, 'stang_leader_activity_copy', None, 'location', '成都', 4, None)
    # sql = "SELECT title, info, id FROM stang_leader_activity WHERE location != ''"
    # S.open_html(None, None, random_nums=10, sql_text=sql)
    S.open_html(8435, 'stang_bid_new', extra_text='测试')