import uuid
from datetime import datetime
from time import time

from SourceCode.table import Schema, Record

from SourceCode.types import *


class TrxState:
    TRX_STATE_NOT_STARTED = 0
    TRX_STATE_ACTIVE = 1
    TRX_STATE_ROLL_BACK = 2
    TRX_COMMITTED = 3


class LogType:
    START = 0
    ACTIVE = 1
    COMMIT = 2
    ROLLBACK = 3
    CHECK_POINT = 4


class OpType:
    INSERT = 0
    DELETE = 1
    UPDATE = 2


def next_lsn():
    uu_id = uuid.uuid4()
    suu_id = ''.join(str(uu_id).split('-'))
    return suu_id


# class Record:
#     def __init__(self, trx_id: str):
#         self.trx_id = trx_id
#         self.table_name = 'employee'
#         self.page_id = 123
#         self.content = ''
#
#     def __len__(self):
#         return len(self.content)

def singleton(cls):
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]

    return inner


@singleton
class LogRecord:
    def __init__(self):
        self.LSN = None
        self.logType = None
        self.trxId = None
        self.opType = None
        self.record_tableName = None
        self.record_page_id = None
        self.before = None
        self.after = None
        self.time = None

    def start_write_buf(self):
        start_dict = {"LSN": "VARCHAR", "logType": "INT32", "trxId": "VARCHAR"}
        start_record = Record(Schema(start_dict))
        start_record.record = {"LSN": TypeVarchar(self.LSN, 32),
                               "logType": TypeInt32(self.logType),
                               "trxId": TypeVarchar(self.trxId, 32)}
        return start_record

    def insert_write_buf(self):
        self.opType = OpType.INSERT
        insert_dict = {"LSN": "VARCHAR", "logType": "INT32", "trxId": "VARCHAR",
                       "opType": "INT32", "record_tableName": "VARCHAR", "record_page_id": "",
                       "after": ""}
        insert_record = Record(Schema(insert_dict))
        insert_record.record = {"LSN": TypeVarchar(self.LSN, 32),
                                "logType": TypeInt32(self.logType),
                                "trxId": TypeVarchar(self.trxId, 32),
                                "opType": TypeInt32(self.opType),
                                "record_tableName": TypeVarchar(self.record_tableName, len(self.record_tableName)),
                                "record_page_id": TypeVarchar(self.record_page_id, len(self.record_page_id)),
                                "after": TypeVarchar(self.after, len(self.after))
                                }
        return insert_record

    def delete_write_buf(self):
        self.opType = OpType.DELETE
        delete_dict = {"LSN": "VARCHAR", "logType": "INT32", "trxId": "VARCHAR",
                       "opType": "INT32", "record_tableName": "VARCHAR", "record_page_id": "",
                       "before": ""}
        delete_record = Record(Schema(delete_dict))
        delete_record.record = {"LSN": TypeVarchar(self.LSN, 32),
                                "logType": TypeInt32(self.logType),
                                "trxId": TypeVarchar(self.trxId, 32),
                                "opType": TypeInt32(self.opType),
                                "record_tableName": TypeVarchar(self.record_tableName, len(self.record_tableName)),
                                "record_page_id": TypeVarchar(self.record_page_id, len(self.record_page_id)),
                                "before": TypeVarchar(self.before, len(self.before))
                                }
        return delete_record

    def update_write_buf(self):
        self.opType = OpType.UPDATE
        update_dict = {"LSN": "VARCHAR", "logType": "INT32", "trxId": "VARCHAR",
                       "opType": "INT32", "record_tableName": "VARCHAR", "record_page_id": "",
                       "before": "", "after": ""}
        update_record = Record(Schema(update_dict))
        update_record.record = {"LSN": TypeVarchar(self.LSN, 32),
                                "logType": TypeInt32(self.logType),
                                "trxId": TypeVarchar(self.trxId, 32),
                                "opType": TypeInt32(self.opType),
                                "record_tableName": TypeVarchar(self.record_tableName, len(self.record_tableName)),
                                "record_page_id": TypeVarchar(self.record_page_id, len(self.record_page_id)),
                                "before": TypeVarchar(self.before, len(self.before)),
                                "after": TypeVarchar(self.after, len(self.after))
                                }
        return update_record

    def commit_write_buf(self):
        commit_dict = {"LSN": "VARCHAR", "logType": "INT32", "trxId": "VARCHAR"}
        commit_record = Record(Schema(commit_dict))
        commit_record.record = {"LSN": TypeVarchar(self.LSN, 32),
                                "logType": TypeInt32(self.logType),
                                "trxId": TypeVarchar(self.trxId, 32)}
        return commit_record

    def check_point_write_buf(self):
        str_content = 'check'
        check_point_dict = {str_content: "VARCHAR"}
        check_point_record = Record(Schema(check_point_dict))
        check_point_record.record = {str_content: TypeVarchar(str_content, len(str_content))}
        return check_point_record

    # def undo(self):
    #     if self.opType == OpType.INSERT:
    #         delete(self.after)
    #     if self.opType == OpType.UPDATE:
    #         update(self.before)
    #     if self.opType == OpType.DELETE:
    #         insert(self.before)


class Trx:
    def __init__(self, trx_id):
        self.trxId = trx_id
        self.state = None

    def begin(self) -> Record:
        start_log = LogRecord()
        start_log.trxId = self.trxId
        start_log.LSN = next_lsn()
        start_log.trxId = self.trxId
        start_log.logType = LogType.START
        self.state = TrxState.TRX_STATE_NOT_STARTED
        return start_log.start_write_buf()

    def add_insert_log(self, after: Record) -> Record:
        log_record = LogRecord()
        log_record.trxId = self.trxId
        log_record.LSN = next_lsn()
        log_record.opType = OpType.INSERT
        log_record.logType = LogType.ACTIVE
        log_record.after = after.record
        self.state = TrxState.TRX_STATE_ACTIVE
        return log_record.insert_write_buf()

    def add_delete_log(self, before: Record) -> Record:
        log_record = LogRecord()
        log_record.trxId = self.trxId
        log_record.LSN = next_lsn()
        log_record.opType = OpType.DELETE
        log_record.logType = LogType.ACTIVE
        log_record.before = before.record
        self.state = TrxState.TRX_STATE_ACTIVE
        return log_record.delete_write_buf()

    def add_update_log(self, before: Record, after: Record) -> Record:
        log_record = LogRecord()
        log_record.trxId = self.trxId
        log_record.LSN = next_lsn()
        log_record.opType = OpType.UPDATE
        log_record.logType = LogType.ACTIVE
        log_record.before = before.record
        log_record.after = after.record
        self.state = TrxState.TRX_STATE_ACTIVE
        return log_record.update_write_buf()

    def commit(self) -> Record:
        commit_log = LogRecord()
        commit_log.trxId = self.trxId
        commit_log.LSN = next_lsn()
        commit_log.trxId = self.trxId
        commit_log.logType = LogType.COMMIT
        self.state = TrxState.TRX_COMMITTED
        return commit_log.commit_write_buf()

    def rollBack(self):
        trx_mgr = TrxMgr()
        trx_mgr.roll_back(self.trxId)


class LogPage:
    def __init__(self):
        self.logs = [LogRecord() for i in range(10)]
        self.dirty = False
        self.page_id = 1
        self.first_log_time = time()  # 第一条记录的时间

    def append_log(self, log):
        self.logs.append(log)


class LogBufferPool:
    def __init__(self, pool_size=8):
        self.pages = [LogPage() for i in range(pool_size)]
        self.pool_size = pool_size

    def append_log(self, log):
        self.pages[0].append_log(log)


buffer_size = 8

def transfer(log_buffer_pool : LogBufferPool):
    pass

class TrxMgr:
    def __init__(self):
        self.log_buffer_pool = LogBufferPool(buffer_size)
        self.trx_dic = {}

    def trx_id_allocator(self):
        uu_id = uuid.uuid4()
        suu_id = ''.join(str(uu_id).split('-'))
        return suu_id

    def begin(self):
        trx_id = self.trx_id_allocator()
        trx = Trx(trx_id)
        log = trx.begin()
        self.log_buffer_pool.append_log(log)
        self.trx_dic[trx_id] = trx
        return trx

    def insert_trx(self, after: Record):
        trx = self.begin()
        insert(trx.trxId, after)
        log = trx.add_insert_log(after)
        self.log_buffer_pool.append_log(log.insert_write_buf())

    def delete_trx(self, before: Record):
        trx = self.begin()
        delete(trx.trxId, before)
        log = trx.add_delete_log(before)
        self.log_buffer_pool.append_log(log.delete_write_buf())

    def update_trx(self, before: Record, after: Record):
        trx = self.begin()
        update(trx.trxId, before, after)
        log = trx.add_update_log(before, after)
        self.log_buffer_pool.append_log(log.update_write_buf())

    def select_trx(self, before: Record):
        trx = self.begin()
        selectFrom(trx.trxId, before)

    def commit_trx(self, commit_record: Record):
        trx = self.trx_dic[commit_record.trx_id]
        log = trx.commit()
        self.log_buffer_pool.append_log(log)

    def undo(self):
        # TODO 需要遍历整个日志
        pass

    # def begin(self):
    #     trx_id = self.trx_id_allocator()
    #     trx = Trx(trx_id)
    #     log = trx.begin()
    #     self.log_buffer_pool.append_log(log)
    #     self.trx_dic[trx_id] = trx


    def commit(self, commit_record: Record):
        trx = self.trx_dic[commit_record.trx_id]
        log = trx.commit()
        self.log_buffer_pool.append_log(log)

    def roll_back_by_trx_id(self, trx_id):
        self.log_buffer_pool.pages.sort(key=lambda log_page: log_page[-1])
        for log_page in self.log_buffer_pool.pages:
            for log in log_page.logs[::-1]:
                if log.trxId == trx_id & log.logType != LogType.START:
                    log.undo()

    def time_cmp(first_time, second_time):
        return int(time.strftime("%H%M%S", first_time)) - int(time.strftime("%H%M%S", second_time))

    def roll_back_db(self, start_time):
        self.log_buffer_pool.pages.sort(key=lambda log_page: log_page[-1])
        for log_page in self.log_buffer_pool.pages:
            if self.time_cmp(log_page.first_log_time, start_time) > 0:
                for log in log_page.logs[::-1]:
                    if log.logType != LogType.COMMIT | log.logType != LogType.CHECK_POINT | log.logType != LogType.START:
                        log.undo()
            else:
                transfer(self.log_buffer_pool)

    def add_check_point(self):
        log = LogRecord()
        log.logType = LogType.CHECK_POINT
        self.log_buffer_pool.append_log(log)


if __name__ == '__main__':
    trx = Trx(next_lsn())
    start = trx.begin()
    print(start.serialize())
    with open("../../TestCase/tableTest.bin", "wb") as f:
        f.write(start.serialize())

    with open("../../TestCase/tableTest.bin", "rb") as f:
        r = Record.unserialize(f.read())
    print(r.record)
