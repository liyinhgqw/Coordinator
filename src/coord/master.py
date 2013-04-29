'''
Created on Apr 1, 2013

@author: liyinhgqw
'''

import time
import logging
import coord.common
import rpc.server
import rpc.client
from rpc.common import pickle, unpickle, RemoteException
import leveldb

class Master(object):
  class MyHandler(object):
    def __init__(self, master):
      self.master = master
      self.jobinfoDB = self.master.db
      
    def do_something(self, arg1, arg2):
      return int(arg1) + int(arg2)
    
    def foo(self, handle, arg1, arg2):
      handle.done(self.do_something(arg1, arg2))
      
    
    # Called from jobconf (client end)
    def register_job(self, handle, jobinfos):
      for jobname, jobinfo in jobinfos.iteritems():
        if jobinfo.has_key('Host'):
          slavehost = jobinfo['Host']
          slavehost.strip()
          if slavehost.startswith('CPU') or slavehost.startswith('MEM'):
            jobinfo['Dynamic'] = slavehost
            jobinfo['Host'] = ''
          else:
            jobinfo['Dynamic'] = False
            
          self.jobinfoDB.Put(jobname, pickle(jobinfo))
        else:
          print 'Must decide which host to run.'
          exit(1)
          
        if self.master.rpc_client.has_key(slavehost):
          self.master.rpc_client[slavehost].register_job(jobname, jobinfo)
          
      print unpickle(self.jobinfoDB.Get(jobname))
      handle.done(1)
      
      
    def lookup(self, handle, jobname):
      try:
        jobinfo_pickled = self.jobinfoDB.Get(jobname)
        handle.done(unpickle(jobinfo_pickled))
      except KeyError:
        handle.done({})
      
    def sync_jobinfo(self, host, port): 
      for (jobname, jobinfo_pickled) in list(self.jobinfoDB.RangeIter(key_from=None, key_to=None)):
        jobinfo = unpickle(jobinfo_pickled)
        slavehost = jobinfo['Host']
        if jobinfo['Dynamic'] != '' or slavehost.startswith('CPU') or slavehost.startswith('MEM'):
          for slave_rpc in self.master.rpc_client.itervalues():
            slave_rpc.register_job(jobname, jobinfo)
        else:
          if self.master.rpc_client.has_key(slavehost):
            self.master.rpc_client[slavehost].register_job(jobname, jobinfo)
          
    # Called from slaves
    def register_slave(self, handle, host, port):
      self.master.logger.info('register slave ' + host + ':' + str(port))
      self.master.rpc_client[host] = rpc.client.RPCClient(host, int(port))
      self.sync_jobinfo(host, int(port));
      handle.done(1)
      
    def find_dynamic_host(self, handle, criteria):
      print '---------------------'
      return handle.done(self.master.find_dynamic_host_wo(criteria))
    
    def set_dynamic_host(self, handle, jobname, host):
      try:
        jobinfo_pickled = self.jobinfoDB.Get(jobname)
        jobinfo = unpickle(jobinfo_pickled)
        jobinfo['Host'] = host
        self.jobinfoDB.Put(jobname, pickle(jobinfo))
        handle.done(True)
      except KeyError:
        handle.done(False)
      
    
  def __init__(self, port):
    self.logger = logging.getLogger("Master")
    self.logger.setLevel(logging.DEBUG)
    self.db = leveldb.LevelDB('jobinfo.db')
    self.rpc_server = rpc.server.RPCServer(coord.common.localhost(), int(port), handler=self.MyHandler(self))
    self.rpc_client = {}
    self.slave_stats = {}
    
  def heartbeat(self):
    bad_slave = [];
    for host, slave in self.rpc_client.iteritems():
      heartbeat_future = slave.heartbeat()
      try:
        if (heartbeat_future.wait()):
          self.logger.info("heatbeat successful")
        else:
          self.logger.info("heatbeat failed")
          bad_slave.append(host) 
      except:
        self.logger.info("heatbeat failed")
        bad_slave.append(host) 
          
    for host in bad_slave:
      self.rpc_client[host].close()
      del self.rpc_client[host]
        
  def server_forever(self):
    while self.running:
      time.sleep(1)
      self.heartbeat()
        
  
  def start(self):
    self.rpc_server.start()
    self.running = True
    self.server_forever()
        
  def stop(self):
    ''' Cannot stop immediately '''
    self.running = False
    
  def get_slave_stats(self):
    print self.rpc_client, "*******"
    for slavehost, slaverpc in self.rpc_client.iteritems():
      print slavehost, "##"
      self.slave_stats[slavehost] = slaverpc.get_slave_stat().wait()
    return self.slave_stats
  
  def get_slave_stat(self, slavehost):
    if self.rpc_client.has_key(slavehost):
      self.slave_stats[slavehost] = self.rpc_client[slavehost].get_slave_stat().wait()
    return self.slave.stats[slavehost]
      
      
  def find_dynamic_host_wo(self, criteria):
    mincpu = None
    minslave = None
    
    for slavehost, slavestat in self.get_slave_stats().iteritems():
      print slavestat, "&&"
      if slavestat.has_key(criteria):
        # TODO: for mem: '>'
        if mincpu is None or slavestat[criteria] < minslave:
          minslave = slavehost
          mincpu = slavestat[criteria]
    print 'minslave = ', minslave
    return minslave

if __name__ == '__main__':
  master = Master(coord.common.MASTER_PORT)
  master.start()

