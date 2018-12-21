import pymysql

class DataSQL():

    def __init__(self):
        self.db = pymysql.connect("192.168.1.252", "root", "123asd123asd", "suidaobig", charset='utf8')
        self.cursor = self.db.cursor()

    def read_data_from_id(self, id, table_name):
        """
        读取数据， 根据id和表名
        :param id: 数据库id
        :param table_name: 数据库表名称
        :return: 数据结构为tuple: (id, title, info)
        """
        sql = """
              SELECT id, title, info 
              FROM {}
              WHERE id = {}
              """.format(table_name, id)
        # 此方法不防注入， 防注入写法会自动给占位符添加引号而导致报错，待解决。
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result

    def read_data_from_id_and_column_names(self, id, *args, table_name='stang_big_fagui'):
        """
        通过id和列名读取数据
        示例：read_data_from_id_and_column_names(4765, 'id', 'title', 'persons', table_name='stang_big_fagui_by_neural_net')
        :param id: id
        :param args: 列名
        :param table_name: 数据库表名
        :return: tuple
        """
        len_args = len(args)
        sql = """
              SELECT """ + self.__format_keys_insert_string(len_args) + """
              FROM {}
              WHERE id = {}
              """
        sql = sql.format(*args, table_name, id)
        self.cursor.execute(sql)
        result = self.cursor.fetchone()
        return result


    def update_with_id_and_column_name(self, id, text, target_column, table_name):
        """
        更新数据库，根据id和列名。进行内容的更新
        :param id: 数据库id
        :param text: 更新后的内容
        :param target_column: 需要更新的列
        :param table_name: 需要更新的表名称
        :return: None
        """
        sql_to_person = """
                        UPDATE {0}
                        SET {4} = {3}{1}{3}
                        WHERE id = {2}
                        """.format(table_name, text, id, "'", target_column)
        self.cursor.execute(sql_to_person)

    def __insert_connections(self, committee_id, bid_id):
        sql_to_connections = """
                             INSERT INTO stang_bid_committee_connect(committee_id, for_id)
                             VALUES (%s, %s)
                             """
        self.cursor.execute(sql_to_connections, (committee_id, bid_id))

    def insert_data_with_table_name(self, table_name, **kwargs):
        """
        向数据库中插入新的数据
        示例：insert_data_with_table_name('stang_big_fagui_by_neural_net', persons='algorithm_aa', institutions='test2')
        采用**kwargs的形式获取参数， 参数名为列名，值为需要被插入的数据
        :param table_name: 数据库表名
        :param kwargs: 列名：数据 键值对
        :return: None
        """
        len_str = len(kwargs)
        keys = kwargs.keys()
        values = kwargs.values()
        sql = """
              INSERT INTO {}(""" + self.__format_keys_insert_string(len_str) + """)
              VALUES (""" + self.__format_values_insert_string(len_str) + """)
              """
        sql = sql.format(table_name, *keys)
        # print(sql)
        self.cursor.execute(sql, tuple(values))

    @staticmethod
    def __format_keys_insert_string(len_str):
        result = '{}, ' * len_str
        return result[:-2]

    @staticmethod
    def __format_values_insert_string(len_str):
        result = '%s, ' * len_str
        return result[:-2]

    def read_ids_bid(self):
        sql = 'SELECT id FROM stang_bid_new WHERE cate_id = 2'
        self.cursor.execute(sql)
        result = self.cursor.fetchall()
        return result

    def __del__(self):
        self.db.commit()
        self.db.close()


if __name__ == '__main__':
    d = DataSQL()
    help(d.read_data_from_id)
    help(d.update_with_id_and_column_name)
    help(d.insert_data_with_table_name)
    # d.insert_data_with_table_name('stang_big_fagui_by_neural_net', persons='algorithm_aa', institutions='test2')
    print(d.read_data_from_id_and_column_names(4765, 'id', 'title', 'persons', table_name='stang_big_fagui_by_neural_net'))
    d.db.commit()
    d.db.close()

