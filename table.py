from SourceCode.types import *
from SourceCode.constants import BLOCK_SIZE
from bplustree import BPlusTree
import os
from SourceCode.utils import *


class Schema(Data):
    def __init__(self, arg: dict):
        self.schema = arg

    def serialize(self):
        return str(self.schema).encode()

    def unserialize(self, data):
        pass

    def checkValidInput(self, input: list):
        for i, j in enumerate(input):
            if not check(self.schema.values()[i], j):
                return False
        return True


def check(type, data):
    if type == "INT32":
        return TypeInt32.check(data)
    # TODO: Finish Check Function


class Record(Data):
    '''
    记录类，一条记录由多种类型的数据组成，使用变长+定长的方式
    4字节总长度+ 4字节schema长度 + schema + head + result
    head: 32位起始位置+32位数据长度
    '''

    def __init__(self, schema: Schema):
        self.schema = schema
        self.record = {}

    def serialize(self):
        length = 0
        headMove = len(self.record) * 8
        head = b''
        result = b''
        for i in self.record.values():
            head += struct.pack('I', (length + headMove))  # 数据起始位置
            head += struct.pack('I', i.length)  # 数据长度
            length += i.length
            result += i.serialize()
        content = self.schema.serialize() + head + result
        schema_length = struct.pack('I', len(self.schema.serialize()))
        content = schema_length + content
        total_length = len(content)
        ret = struct.pack('I', total_length) + content
        return ret

    @classmethod
    def unserialize(self, data):
        ret = Record(None)
        if not isinstance(data, bytes):
            raise ValueError("unserialize has to have bytes input")
        else:
            total_length = struct.unpack("I", data[0:4])[0]
            schema_length = struct.unpack("I", data[4:8])[0]
            schema = eval(data[8:8 + schema_length].decode())
            ret.schema = schema
            # print(total_length, schema_length)

            for i, j in enumerate(schema.keys()):
                dataHeader = data[8 + schema_length + i * 8:8 + schema_length + (i + 1) * 8]
                dataStartPlace = struct.unpack("I", dataHeader[0:4])[0]
                dataLength = struct.unpack("I", dataHeader[4:8])[0]
                # print([i, j, dataStartPlace, dataLength])
                ret.record[j] = unserialize(data[8 + schema_length + dataStartPlace:
                                                 8 + schema_length + dataStartPlace + dataLength], schema[j],
                                            dataLength)

            # print(ret.record)
        return ret


class Block(Data):
    """
    数据块类，多个数据块组成一个表，磁盘中结构和内存中结构相同，具有序列化反序列化的操作
    """

    def __init__(self):
        self.data = '0'.encode('utf-8') * BLOCK_SIZE  # 初始化块内空间为BLOCK_SIZE个字节 ， utf-8编码英文和数字1字节，中文3字节
        self.freeSize = BLOCK_SIZE
        self.records = []
        self.num_records = None
        self.shifts = {}

    def serialize(self):
        # 从前向后写头信息，从后向前写数据信息
        self.data = bytearray(self.data)
        self.data[0:4] = struct.pack('I', len(self.records))  # 块内记录数量4字节
        endPlace = self.freeSize

        for i, j in enumerate(self.records):
            bdata = bytearray(j.serialize())
            datalength = len(bdata)
            dataStart = self.freeSize - datalength
            endPlace = self.freeSize - datalength
            self.freeSize -= datalength
            self.data[dataStart:dataStart + datalength] = bdata
            pointer = struct.pack("I", dataStart) + struct.pack("I", datalength)
            self.data[4 + i * 8:4 + i * 8 + 8] = pointer

        self.data = bytes(self.data)
        return self.data

    @classmethod
    def unserialize(self, data):
        ret = Block()
        if not isinstance(data, bytes):
            raise ValueError("unserialize has to have bytes input")
        else:
            records_num = struct.unpack("I", data[0:4])[0]
            #print(records_num)
            for i in range(records_num):
                data_start = struct.unpack("I", data[4 + i * 8:4 + i * 8 + 4])[0]
                data_length = struct.unpack("I", data[4 + i * 8 + 4:4 + i * 8 + 8])[0]
                ret.records.append(Record.unserialize(data[data_start:data_start + data_length]))
        return ret


class Index(Data):
    def __init__(self, tablePath):
        self.tree = BPlusTree(os.path.join(tablePath, "Index", "bplustree"))
        # index的value值为block名+偏移量
        blocks = os.listdir(os.path.join(tablePath, "Data"))
        for i in blocks:
            with open(os.path.join(tablePath, "Data", i), "rb") as f:
                tempBlock = Block.unserialize(f.read())
                for j,m in enumerate(tempBlock.records):
                    self.tree.insert(m.record['id'],(i+"@"+str(j)).encode())
        self.tree.close()

    def serialize(self):
        pass

    @classmethod
    def unserialize(self, data):
        return BPlusTree(data)

@timeLog
def bpTreetest():
    # index = Index.unserialize(
    #     "/Users/leo/Desktop/作业/DatabaseImplementation/DatabaseImplement/TestCase/Databases/Bank/Employee/Index/bplustree")
    # print(index[23])
    # index.close()
    print("btTree test Done")

@timeLog
def rawTest():
    for i in range(25):
        with open("../TestCase/Databases/Bank/Employee/Data/" + "block" + str(i) + ".block", "rb")as f:
            block = Block.unserialize(f.read())
            if block.records[0].record['id']==23:
                print("raw Test done")
                return



class baseCondition:
    def __init__(self,command:str):
        pass


    def check(self,record:Record):






class Condition:
    def __init__(self):
        pass

    def check(self):


if __name__ == '__main__':
    bpTreetest()
    rawTest()

    # a = Index("../TestCase/Databases/Bank/Employee")
    #index = Index.unserialize("/Users/leo/Desktop/作业/DatabaseImplementation/DatabaseImplement/TestCase/Databases/Bank/Employee/Index/bplustree")
    #
    # for key,value in index.items():
    #     print(key,value)
    #
    #
    # index.insert(33,b"insert_new")
    # for key,value in index.items():
    #     print(key,value)

    # index[10] = bytes("".encode())
    # for key,value in index.items():
    #     print(key,value)
    # print(index)
    #
    # index.close()


    # a = Record(Schema({"age": "INT32", "name": "CHAR", "email": "VARCHAR"}))
    # a.record = {"age": TypeInt32(32), "name": TypeChar("chenliang"), "email": TypeVarchar("547205480@qq.com", 16)}
    # b = Record(Schema({"age": "INT32", "name": "CHAR", "email": "VARCHAR"}))
    # b.record = {"age": TypeInt32(32), "name": TypeChar("chenliang"), "email": TypeVarchar("547205480@qq.com", 16)}
    #
    # block = Block()
    # block.records.append(a)
    # block.records.append(b)
    # print(block.serialize())
    #
    # q = block.unserialize(block.serialize())
    #
    # import random
    #
    # for i in range(25):
    #     with open("../TestCase/Databases/Bank/Employee/Data/" + "block" + str(i) + ".block", "wb")as f:
    #         new_record = Record(Schema({"id": "INT32", "age": "INT32", "name": "CHAR", "email": "VARCHAR"}))
    #         new_record.record = {"id": TypeInt32(i),
    #                              "age": TypeInt32(random.randint(18, 70)),
    #                              "name": TypeChar(random.choice(["Jack", "Bob", "Leo", "Kevin"])),
    #                              "email": TypeVarchar("547205480@qq.com", 16)}
    #         block = Block()
    #         block.records.append(new_record)
    #         f.write(block.serialize())

    # for i in range(25):
    #     with open("../TestCase/Databases/Bank/Employee/Data/" + "block" + str(i) + ".block", "rb")as f:
    #         block = Block.unserialize(f.read())
