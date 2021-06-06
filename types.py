import struct

class Data:
    def serialize(self):
        raise NotImplementedError
    @classmethod
    def unserialize(self,data):
        raise NotImplementedError


class TypeInt32(Data):
    """
    32位有符号整数，长度为四个字节
    """
    def __init__(self,data):
        super(TypeInt32, self).__init__()
        self.types = "INT32"
        self.length = 4
        if isinstance(data,bytes):
            self.data = struct.unpack('i',data)[0]
        else:
            self.data = data

    def serialize(self):
        return struct.pack('i',self.data)

    @classmethod
    def check(cls,data):
        if not isinstance(data,int):
            return False
        elif data>32657:
            return False
        elif data<-32658:
            return False
        return True

    def __str__(self):
        return str(self.data)


class TypeChar(Data):
    '''
    定长类型Char，长度为256字节，不足256字节在前方用空格补齐，要求数据第一个字符不是空格
    '''
    def __init__(self,data):
        super(TypeChar, self).__init__()
        self.types = "CHAR"
        self.length = 256
        if isinstance(data, bytes):
            self.data = data.decode().strip()
        else:
            self.data = data
    def serialize(self):
        return (" "*(256-len(self.data))).encode()+self.data.encode()

    def __str__(self):
        return self.data


class TypeVarchar(Data):
    '''
    变长类型VARCHAR，需指定长度
    '''
    def __init__(self,data,length):
        super(TypeVarchar, self).__init__()
        self.types = "VARCHAR"
        self.length = length
        if isinstance(data, bytes):
            self.data = data.decode()
        else:
            self.data = data

    def serialize(self):
        return self.data.encode()

    def __str__(self):
        return self.data

def unserialize(data,type,length = None):
    if type == "VARCHAR":
        if not length:
            raise ValueError("length can't be none when unserializing VARCHAR")
        return TypeVarchar(data,length).data
    if type == "CHAR":
        return TypeChar(data).data
    if type == "INT32":
        return TypeInt32(data).data

    return "Not a valid data"

if __name__ == '__main__':
    a = TypeInt32(2442)
    b = TypeChar("hello")
    c = TypeVarchar("547205480@qq.com",100)

    with open("../TestCase/typesTest.bin","wb") as f:
        f.write(a.serialize()+b.serialize()+c.serialize())

    with open("../TestCase/typesTest.bin", "rb") as f:
        print(unserialize(f.read(4),"INT32"))
        print(unserialize(f.read(256),"CHAR"))
        print(unserialize(f.read(16),"VARCHAR",16))

