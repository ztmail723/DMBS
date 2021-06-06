#!/usr/bin/env python
# coding: utf-8

# In[7]:


import time
import threading
class LockManager(object):
    _instance_lock = threading.Lock()

    def __init__(self,size):
        self.lock=threading.Lock()
        LWLockArray=[LWLock(i) for i in range(size)]
        self.size=size;
        self.num=0
        time.sleep()

    def LWLockAssign(self):
        with self.lock:
            if num==size:
                raise RuntimeError('LWLockArray is full can not get more LWLock')
                return False
            else:
                num++
                return LWLockArray[num-1]
    
    def LWLockAcquire(self,ID,ReadORWrite):
        for lock in LWLockArray:
            if lock.LockID==ID:
                if ReadORWrite:
                    lock.read_acquire()
                else:
                    lock.write_acquire()
        raise RuntimeError('noID')
                    
    def LWLockRelease(self,ID):
        for lock in LWLockArray:
            if lock.LockID==ID:
                lock.unlock()
        raise RuntimeError('noID')
        
    @classmethod
    def instance(self, *args, **kwargs):
        if not hasattr(LockManager, "_instance"):
            with LockManager._instance_lock:
                if not hasattr(LockManager, "_instance"):
                    LockManager._instance = LockManager(*args, **kwargs)
        return LockManager._instance


# In[ ]:





# In[ ]:




