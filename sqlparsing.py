import sqlparse

class SqlParsing:
    def __init__(self):
        self.sql = "select A from B where C"
        self.parsed = sqlparse.parse(self.sql)

    def set_sql(self, sql_str):
        self.sql = sql_str
        self.parsed = sqlparse.parse(self.sql)

    def get_sql(self):
        return self.sql

    def parse_sql(self):
        sql_type = self.parsed[0].get_type()
        if sql_type == 'SELECT':
            ans = self.__parse_select()
            self.run_select(ans)
        elif sql_type == 'UPDATE':
            ans = self.__parse_update()
            self.run_update(ans)
        elif sql_type == 'INSERT':
            ans = self.__parse_insert()
            self.run_insert(ans)
        elif sql_type == 'DELETE':
            ans = self.__parse_delete()
            self.run_delete(ans)
        elif sql_type == 'CREATE':
            ans = self.__parse_create()
            self.run_create(ans) 


    def __parse_update(self):
        update_list = []
        flag = 0
        for item in self.parsed[0].tokens:
            if flag == 0:
                if item.ttype is None:
                    flag = 1
                    update_list.append(item.value)
            elif flag == 1:
                if item.ttype is None:
                    flag = 2
                    update_list.append(item.value)
            elif flag == 2:
                if item.ttype is None:
                    flag = 3
                    update_list.append(item.value)
        return update_list

    def __parse_insert(self):
        insert_list = []
        attrs_list = []
        flag = False
        for item in self.parsed[0].tokens:
            if not flag:
                if item.ttype is None:
                    flag = True
                    insert_list.append(item.value)
            else:
                if item.ttype is None:
                    for item2 in item:
                        if item2.ttype is None:
                            for item3 in item2:
                                if item3.ttype is None:
                                    for item4 in item3:
                                        if item4.ttype is sqlparse.tokens.Token.Literal.String.Single:
                                            temp4 = item4.value[1:-1]
                                            attrs_list.append(temp4)

        insert_list.append(attrs_list)
        return insert_list

    def __parse_delete(self):
        delete_list = []
        flag = False
        for item in self.parsed[0].tokens:
            if not flag:
                if item.ttype is None:
                    flag = True
                    delete_list.append(item.value)
            else:
                if item.ttype is None:
                    delete_list.append(item.value)
                    return delete_list
        return delete_list

    def __parse_create(self):
        table_list = []
        attrs_list = []
        flag_turn_outer = False
        for item in self.parsed[0].tokens:
            if not flag_turn_outer:
                if item.ttype is None:
                    table_list.append(item.value)
                    flag_turn_outer = True
            else:
                if item.ttype is None:
                    attr_list = []
                    flag_turn = False
                    for item2 in item:
                        if not flag_turn:
                            if item2.ttype is None:
                                attr_list.append(item2.value)
                                flag_turn = True
                        else:
                            if item2.ttype is sqlparse.tokens.Token.Name.Builtin:
                                attr_list.append(item2.value)  # 一个列表对
                                attrs_list.append(attr_list[:])
                                attr_list.clear()
                                flag_turn = False
                    table_list.append(attrs_list[:])
        return table_list