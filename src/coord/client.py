'''
Created on Apr 3, 2013

@author: liyin
'''

import sys
import os
import time
import logging
import rpc.server
import rpc.client
from rpc.common import pickle, unpickle, RemoteException
import coord.common
import leveldb
import threading
from functools import partial

class _MethodCall(object):
    def __init__(self, client, name):
      self.client = client
      self.name = name

    def __call__(self, jobname, *args, **kw):
      return self.client.call(self.name, jobname, *args, **kw)


class Client(object):
  def __init__(self, master, wfname = None):
    self.master = master
    self.recovery = False
    self.recovered = False
    self.wfname = wfname
    host, port = rpc.common.split_addr(master)
    self.rpc_client = rpc.client.RPCClient(host, port)
    self.rpc_slave = {}
    
  def __getattr__(self, key):
    return _MethodCall(self, key)
    
  
  def _get_slave_rpc(self, jobinfo):
    host = jobinfo['Host']
    if not self.rpc_slave.has_key(host):
      self.rpc_slave[host] = rpc.client.RPCClient(host, coord.common.SLAVE_PORT)
    return self.rpc_slave[host]
    
  def cmd(self, cmdstr):
    return self.rpc_client.cmd(cmdstr).wait()
  
  def lookup(self, jobname):
    ret = self.rpc_client.lookup(jobname).wait()
    if isinstance(ret, dict):
      return ret
    else:
      raise Exception
    
  def getinfo(self, jobname, info):
    return self.lookup(jobname)[info]
  
  # Equavalent to client_rpc.func(jobname)
  def call(self, func, jobname, *args, **kw):
    jobinfo = self.lookup(jobname)
    call_future = self._get_slave_rpc(jobinfo).call(func, jobname, *args, **kw)
    return call_future.wait()
  
  # support recovery
  def execute(self, jobname, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      print 'exec'
      self.call('execute', jobname, check_finished, *args, **kw)
      if self.recovery:
        self.log_recovery(jobname)
      
  # support recovery
  # diverse convenient execute funtion
  def execute_cond(self, jobname, cond, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      if cond(jobname):
        print 'exec'
        self.call('execute', jobname, check_finished, *args, **kw)
      if self.recovery:
        self.log_recovery(jobname)
  
  # support recovery
  def execute_cond_wait(self, jobname, cond, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      while not cond(jobname):
        time.sleep(lfs = coord.common.LFS())
      print 'exec'
      self.call('execute', jobname, check_finished, *args, **kw)
      if self.recovery:
        self.log_recovery(jobname)    
  
  # support recovery
  def execute_dep(self, jobname, depname, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      while not self.call('check_milestone', depname, *args, **kw):
        time.sleep(coord.common.RETRY_INTERVAL)
      print 'exec'
      self.call('execute', jobname, check_finished,  *args, **kw)
      if self.recovery:
        self.log_recovery(jobname)      
  
  def execute_period(self, jobname, interval=1.0, check_finished = True, *args, **kw):
    # do not check finished for the first round
    self.call('execute', jobname, check_finished, *args, **kw)
    _period_execute = partial(self.execute_period, jobname, interval, check_finished, *args, **kw)
    t = threading.Timer(interval, _period_execute)
    t.start()
  
  def execute_period_cond(self, jobname, cond, interval, check_finished = True, *args, **kw):
    self.execute_cond(jobname, cond, check_finished, *args, **kw)
    _period_execute = partial(self.execute_period_cond, jobname, cond, interval, check_finished, *args, **kw)
    t = threading.Timer(interval, _period_execute)
    t.start()
    
  def check_recovery(self, jobname):
    if not self.recovery or self.recovered:
      return False
    try:
      if not self.cter.has_key(jobname):
        cnt = self.db.Get(jobname)
        self.cter[jobname] = cnt
      if self.cter.has_key(jobname) and self.cter[jobname] > 0:
        --self.cter[jobname]
        return True
      else:
        return False
    except KeyError:
      return False
      
      
  def log_recovery(self, jobname):
    self.recovered = True
    counter = self.db.Get(jobname)
    self.db.Put(jobname, ++counter)
    
  def begin_checkblock(self, name):
    self.db = leveldb.LevelDB(name + '.db')
    self.recovery = True
    self.recovered = False
    self.cter = {}
    
  def end_checkblock(self, name):
    if os.path.exists(name + '.db'):
      os.rmdir(name + '.db')
    
if __name__ == '__main__':
  sockname = coord.common.localhost() + ':' + str(coord.common.MASTER_PORT)
  client = Client(sockname)
#  print client.lookup('Job1')
#  print client.call('execute', 'Job1')
#  print client.call('get_input_size', 'Job1')
#  print client.call('get_input_totalsize', 'Job1')
#  print client.call('get_input_subdirnum', 'Job1')
  print client.call('get_input_subdirtotalnum', 'Job1')
#  client.call('period_execute', 'Job1', 5.0)  
  client.call('execute', 'Job1')
