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
    self.rpc_client = rpc.client.RPCClient(master, coord.common.MASTER_PORT)
    self.rpc_slave = {}
    
  def __getattr__(self, key):
    return _MethodCall(self, key)
    
  
  def _get_slave_rpc(self, host):
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
    
  def find_dynamic_host(self, criteria):
    return self.rpc_client.find_dynamic_host(criteria).wait()
  
  def set_dynamic_host(self, jobname, host):
    return self.rpc_client.set_dynamic_host(jobname, host).wait()
    
  def getinfo(self, jobname, info):
    return self.lookup(jobname)[info]
  
  # Equavalent to client_rpc.func(jobname)
  def call(self, func, jobname, *args, **kw):
    jobinfo = self.lookup(jobname)
    print jobinfo
    host = jobinfo['Host']
    
    # check if set dynamic
    if jobinfo.has_key('Dyanmic') and jobinfo['Dynamic'] is not None and func == 'execute':
      if host is None or host == '':
        host = self.find_dynamic_host(jobinfo['Dynamic'])
        self.set_dynamic_host(jobname, host)
      else:
        # check if set check_finished
        if True in args:
          if self.call('is_running', jobname):
            return None
          else:
            host = self.find_dynamic_host(jobinfo['Dynamic'])
            print 're-assign to slave: ', host
            self.set_dynamic_host(jobname, host)
    
    if host is None or host == '':
      return None
    call_future = self._get_slave_rpc(host).call(func, jobname, *args, **kw)
    return call_future.wait()
  
  # support recovery
  def execute(self, jobname, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      print 'exec'
      self.call('execute', jobname, check_finished)
      if self.recovery:
        self.log_recovery(jobname)
      
  # support recovery
  # diverse convenient execute funtion
  def execute_cond(self, jobname, cond, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      if cond(jobname, *args, **kw):
        print 'exec', jobname
        self.call('execute', jobname, check_finished)
      if self.recovery:
        self.log_recovery(jobname)
  
  # support recovery
  def execute_cond_wait(self, jobname, cond, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      while not cond(jobname, *args, **kw):
        time.sleep(lfs = coord.common.LFS())
      print 'exec'
      self.call('execute', jobname, check_finished)
      if self.recovery:
        self.log_recovery(jobname)    
  
  # support recovery
  def execute_dep(self, jobname, depname, check_finished = True, *args, **kw):
    if not self.recovery or not self.check_recovery(jobname):
      while not self.call('is_milestone', depname):
        time.sleep(coord.common.RETRY_INTERVAL)
      print 'exec'
      self.call('execute', jobname, check_finished)
      if self.recovery:
        self.log_recovery(jobname)      
  
  def execute_period(self, jobname, interval=1.0, check_finished = True, *args, **kw):
    # do not check finished for the first round
    self.call('execute', jobname, check_finished)
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
    counter += 1
    self.db.Put(jobname, counter)
    
  def begin_checkblock(self, name):
    self.db = leveldb.LevelDB(name + '_recovery.db')
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
