sql_1 = "CREATE TABLE Persons"\
"("\
"Id_P int, "\
"LastName varchar, "\
"FirstName varchar, "\
"Address varchar, "\
"City varchar"\
")"
sql_2 = "DELETE FROM Person WHERE LastName = \'Wilson\' and a = 1"
sql_3 = "DELETE FROM Person"
sql_4 = "INSERT INTO Persons VALUES ('Gates', 'Bill', 'Xuanwumen 10', 'Beijing') " 
sql_5 = "UPDATE Person SET Address = 'Zhongshan 23', City = 'Nanjing' WHERE LastName = 'Wilson' " 
tst_1 = SqlParsing()
tst_1.set_sql(sql_1)
tst_1.parse_sql()
tst_2 = SqlParsing()
tst_2.set_sql(sql_2)
tst_2.parse_sql()
tst_3 = SqlParsing()
tst_3.set_sql(sql_3)
tst_3.parse_sql()
tst_4 = SqlParsing()
tst_4.set_sql(sql_4)
tst_4.parse_sql()
tst_5 = SqlParsing()
tst_5.set_sql(sql_5)
tst_5.parse_sql()