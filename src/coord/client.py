'''
Created on Apr 3, 2013

@author: liyin
'''


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
  def __init__(self, master):
    self.master = master
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
  
  # diverse convenient execute funtion
  def execute_cond(self, jobname, cond, *args, **kw):
    if cond(jobname):
      print 'exec'
      self.call('execute', jobname, *args, **kw)
  
  def execute_period(self, jobname, interval=1.0, *args, **kw):
    # do not check finished for the first round
    self.call('execute', jobname, *args, **kw)
    _period_execute = partial(self.execute_period, jobname, interval, *args, **kw)
    t = threading.Timer(interval, _period_execute)
    t.start()
  
  def execute_period_cond(self, jobname, cond, interval, *args, **kw):
    self.execute_cond(jobname, cond, *args, **kw)
    _period_execute = partial(self.execute_period_cond, jobname, cond, interval, *args, **kw)
    t = threading.Timer(interval, _period_execute)
    t.start()
    
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
