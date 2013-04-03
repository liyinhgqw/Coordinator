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
    print host, coord.common.SLAVE_PORT
    return self.rpc_slave[host]
    
  
  def lookup(self, jobname):
    ret = self.rpc_client.lookup(jobname).wait()
    if isinstance(ret, dict):
      return ret
    else:
      raise Exception

  def execute(self, jobname):
    jobinfo = self.lookup(jobname)
    exec_future = self._get_slave_rpc(jobinfo).execute(jobinfo['Command'])
    return exec_future.wait()
  
  def getinfo(self, jobname, info):
    return self.lookup(jobname)[info]
    
    

if __name__ == '__main__':
  client = Client('216.165.108.67:9999')
  client.lookup('Job1')
  client.lookup('Job4')
  client.execute('Job1')