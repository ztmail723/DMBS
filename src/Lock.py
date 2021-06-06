#!/usr/bin/env python
# coding: utf-8

# In[4]:


import threading
class LWLock(object):
    def _init_(self，ID):
        self.lock=threading.Lock()
        self.rcond=threading.Condition(self.lock)
        self.wcond=threading.Condition(self.lock)
        self.LockID=ID
        self.read_waiter=0                                 #读线程数
        self.write_waiter=0                                #写线程数
        slef.state=0                                       #正数表示读线程，负数表示写线程
        self.owners=[]                                     #正在操作的线程ID集合
        self.write_first=true                              #默认写优先，false时读优先
        
    def write_acquire(self,blocking=True):                 #blocking=true时当前线程进入阻塞队列，否则不进入
        me=threading.get_ident()
        with self.lock:
            while not self._write_acquire(me):
                if not blocking:
                    return False
                self.write_waiter+=1
                self.wcond.wait()
                self.write_waiter-=1
        return True
    
    def _write_acquire(self,me):
        if self.state==0 or (self.state < 0 and me in slef.owners):
            self.state -= 1 
            self.owners.append(me)
            return True
        if self.state > 0 and me in self.owners:
            raise RuntimeError('cannot recursively wrlock a rdlocked lock')
            return False
        
    def read_acquire(self,blocking=True):
        me = threading.get_ident()
        with self.lock():
            while not self._read_acquire(me):
                if not blocking:
                    return False
                self.read_waiter+=1
                self.rcond.wait()
                self.read_waiter-=1
        return True
    
    def _read_acquire(slef,me):
        if self.state<0:
            return False
        if not self.write_waiter:
            ok=True
        else:
            ok=me in self.owners
        if ok or not self.write_first:
            self.state+=1
            self.owners.append(me)
            return True
        return False
    
    def unlock(self):
        me=threading.get_ident()
        with self.lock:
            try:
                self.owners.remove(me)
            except ValueError:
                raise RuntimeError('cannot release un-acquired lock')
            if self.state>0:
                self.state-=1
            else:
                self.state+=1
            if not self.state:
                if self.write_waiter and self.write_first:
                    self.wcond.notify()
                elif self.read_waiter:
                    self.rcond.notify_all()
                elif self.write_waiter:
                    self.wcond.notify()
    read_release=unlock
    write_release=unlock
    


# In[ ]:





# In[ ]:




