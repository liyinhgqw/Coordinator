'''
Created on Apr 1, 2013

@author: liyinhgqw
'''

import time
import logging
import rpc.server
import rpc.client

class Master(object):
  class MyHandler(object):
    def __init__(self, master):
      self.master = master
      
    def do_something(self, arg1, arg2):
      return int(arg1) + int(arg2)
    
    def foo(self, handle, arg1, arg2):
      handle.done(self.do_something(arg1, arg2))
      
    def register_slave(self, handle, host, port):
      self.master.rpc_client[host] = rpc.client.RPCClient(host, int(port))
      handle.done(1)
    
  def __init__(self, port):
    self.logger = logging.getLogger("Master")
    self.logger.setLevel(logging.DEBUG)
    self.rpc_server = rpc.server.RPCServer('localhost', int(port), handler=self.MyHandler(self))
    self.rpc_client = {}
    
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
      

if __name__ == '__main__':
  master = Master(9999)
  master.start()

