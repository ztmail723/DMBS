import os

from .constants import DB_Repo

def createDatabase(name: str):
    if name not in os.listdir(DB_Repo):
        os.makedirs(os.path.join(DB_Repo,name))
        print(name, "数据库创建成功")
    else:
        print("有重名数据库，请重新创建")


def createTable(database: str, name: str):
    if name not in os.listdir(os.path.join(DB_Repo,database)):
        os.makedirs(os.path.join(DB_Repo,database,name))
        os.makedirs(os.path.join(DB_Repo,database,name,"Schema"))
        os.makedirs(os.path.join(DB_Repo,database, name, "Data"))
        os.makedirs(os.path.join(DB_Repo,database, name, "Index"))
    else:
        print("有重名表，请重新创建")

def createSchema(database:str,table:str,schema:dict):
    pass

def selectFrom(colNames:list,database,table):
    pass

def join(table1:str,table2:str):
    pass

def update(table:str,id:int,record):
    pass

def delete(table:str,id:int):
    pass

def add(table:str,record):
    pass







if __name__ == '__main__':
    createDatabase(
        "Bank"
    )

    createTable("School","teaching")
    createTable("Bank", "Employee")
    createTable("Bank", "Salary")
