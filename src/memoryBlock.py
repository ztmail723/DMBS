import threading
import struct
from types import *

from SourceCode.constants import BLOCK_NUM, BLOCK_SIZE
from SourceCode.utils import memLog, timeLog
from SourceCode.table import Block


class Pointer:
    types = {'NULL': b'00', 'SHIFT': b"01", "DISK": b"10", "MEMORY": b"11"}

    def __init__(self):
        self.type = b''
        self.data = b''

    def serialize(self):
        raise NotImplementedError


class NullPointer(Pointer):
    def serialize(self):
        pass


class ShiftPointer(Pointer):
    def __init__(self, dataStart: int, dataLength: int):
        super(ShiftPointer, self).__init__()
        self.type = Pointer.types['SHIFT']
        self.dataStart = dataStart
        self.dataLength = dataLength

    def serialize(self):
        return self.type + struct.pack('i', self.dataStart) + struct.pack('i', self.dataLength)


class Record:
    pass


class MemoryWatcher:
    pass


class MemoryBlock:
    """
    内存块类
    """

    def __init__(self, BLOCK_SIZE, id):
        self.data = '0'.encode('utf-8') * BLOCK_SIZE  # 初始化块内空间为BLOCK_SIZE个字节 ， utf-8编码英文和数字1字节，中文3字节
        self.max_size = BLOCK_SIZE
        self.used = False
        self.id = id

    def initailize(self):
        self.data = '0'.encode('utf-8') * BLOCK_SIZE  # 初始化块内空间为BLOCK_SIZE个字节 ， utf-8编码英文和数字1字节，中文3字节
        self.max_size = BLOCK_SIZE
        self.used = False
        self.id = id

    def addRecord(self, Record: Record):
        pass

    def deleteRecord(self, RecordIndex: int):
        pass

    def getRecord(self, RecotdIndex: int):
        pass

    def serialize(self, data):
        pass

    def unserialize(self):
        pass


class DB_Cache:
    """
    数据缓冲区类
    """
    # 线程安全的单例确保所有表共享一个内存，不会有多个内存
    _instance_lock = threading.Lock()

    @memLog
    def __init__(self):
        self.memoryblocks = [MemoryBlock(BLOCK_SIZE, i) for i in range(BLOCK_NUM)]
        self.seqTable = {str(i): "" for i in range(BLOCK_NUM)}
        self.reverseTable = {}
        self.LRU_list = []
        self.CLOCK_List = [0 for i in range(BLOCK_NUM)]
        self.clock_tick = 0

    def add_Block_LRU(self, block: Block):  # 找空闲位置
        # 查找是否出现过
        if block in self.reverseTable.keys():
            # 更新LRU
            temp = self.reverseTable[block]
            self.LRU_list.remove(temp)
            self.LRU_list.append(
                temp
            )
            return self.memoryblocks[self.reverseTable[block]]
        else:
            # 查是否有空位
            for i in self.seqTable.keys():
                if self.seqTable[i] == "":
                    self.memoryblocks[int(i)] = block
                    self.seqTable[i] = block
                    self.reverseTable[block] = int(i)
                    # 更新LRU
                    self.LRU_list.append(int(i))
                    return self.memoryblocks[int(i)]
            # 没有空闲需要替换，使用LRU算法
            print("写回页面", self.LRU_list[0])  # 写回最近最少使用的页面
            del self.reverseTable[self.memoryblocks[self.LRU_list[0]]]  # 反表删除页面
            self.memoryblocks[self.LRU_list[0]] = block  # 写入新数据
            self.seqTable[str(self.LRU_list[0])] = block  # 更新顺表
            temp = self.LRU_list[0]
            self.LRU_list.remove(self.LRU_list[0])
            self.LRU_list.append(
                temp
            )
            return self.memoryblocks[self.LRU_list[0]]

    def add_Block_CLOCK(self, block: Block):  # 找空闲位置
        # 查找是否出现过
        if block in self.reverseTable.keys():
            # 更新CLOCK
            temp = self.reverseTable[block]
            self.CLOCK_List[temp] = 1
            return self.memoryblocks[self.reverseTable[block]]
        else:
            # 查是否有空位
            for i in self.seqTable.keys():
                if self.seqTable[i] == "":
                    self.memoryblocks[int(i)] = block
                    # 更新CLOCK
                    self.CLOCK_List[int(i)] = 1
                    self.seqTable[i] = block
                    self.reverseTable[block] = int(i)
                    return self.memoryblocks[int(i)]
            # 没有空闲需要替换，使用CLOCK算法
            for i in range(BLOCK_NUM):
                if self.CLOCK_List[self.clock_tick] == 1:
                    self.CLOCK_List[self.clock_tick] = 0
                    self.clock_tick += 1
                    self.clock_tick = self.clock_tick%BLOCK_NUM # 取模来模拟循环时钟
                if self.CLOCK_List[self.clock_tick] == 0:
                    # 替换页面
                    print("写回页面", self.clock_tick)  # 写回页面
                    del self.reverseTable[self.memoryblocks[self.clock_tick]]  # 反表删除页面
                    self.memoryblocks[self.clock_tick] = block  # 写入新数据
                    self.seqTable[str(self.clock_tick)] = block  # 更新顺表
                    self.CLOCK_List[self.clock_tick]=1
                    self.clock_tick +=1
                    self.clock_tick = self.clock_tick % BLOCK_NUM  # 取模来模拟循环时钟
                    return block

    def __new__(cls, *args, **kwargs):
        if not hasattr(DB_Cache, "_instance"):
            with DB_Cache._instance_lock:
                if not hasattr(DB_Cache, "_instance"):
                    DB_Cache._instance = object.__new__(cls)
        return DB_Cache._instance

if __name__ == '__main__':
    blocks = [Block() for i in range(30)]
    print(blocks)
    cache = DB_Cache()

    print("反置页表",cache.reverseTable)
    a=cache.add_Block_CLOCK(blocks[0])
    print("反置页表",cache.reverseTable)
    b=cache.add_Block_CLOCK(blocks[0])
    print("反置页表",cache.reverseTable)

    print(a==b)


