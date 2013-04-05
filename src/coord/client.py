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

class Client(object):
  def __init__(self, master):
    self.master = master
    host, port = rpc.common.split_addr(master)
    self.rpc_client = rpc.client.RPCClient(host, port)
    self.rpc_slave = {}
  
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
  def call(self, func, jobname):
    jobinfo = self.lookup(jobname)
    call_future = self._get_slave_rpc(jobinfo).call(func, jobname)
    return call_future.wait()
  
if __name__ == '__main__':
  sockname = coord.common.localhost() + ':' + str(coord.common.MASTER_PORT)
  client = Client(sockname)
  print client.lookup('Job1')
  print client.call('execute', 'Job1')
  print client.call('get_input_size', 'Job1')
  print client.call('get_input_totalsize', 'Job1')
  print client.call('get_input_subdirnum', 'Job1')
  print client.call('get_input_subdirtotalnum', 'Job1')
  
