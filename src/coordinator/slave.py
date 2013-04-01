'''
Created on Apr 1, 2013

@author: liyinhgqw
'''
import time
import socket
import logging
import rpc.server
import rpc.client

class Slave(object):
  class MyHandler(object):
    def __init__(self, slave):
      self.slave = slave
      
    def do_something(self, arg1, arg2):
      return int(arg1) + int(arg2)
    
    def foo(self, handle, arg1, arg2):
      handle.done(self.do_something(arg1, arg2))
      
    def heartbeat(self, handle):
      handle.done(True)
    
  def __init__(self, host, port):
    self.logger = logging.getLogger("Slave")
    self.logger.setLevel(logging.DEBUG)
    self._port = port + 1   # slave server listen the port+1
    self.rpc_server = rpc.server.RPCServer('localhost', self._port, handler=self.MyHandler(self))
    self.rpc_client = rpc.client.RPCClient(host, port)
    
  def server_forever(self):
    while self.running:
      time.sleep(100)
  
  def start(self):
    self.rpc_server.start()
    register_slave_future = self.rpc_client.register_slave('127.0.0.1', int(self._port))
    assert 1 == register_slave_future.wait()

    self.logger.info("Registered slave %s:%d" % (socket.gethostname(), self._port))
    
    self.running = True
    self.server_forever()
        
  def stop(self):
    ''' Cannot stop immediately '''
    self.running = False
      

if __name__ == '__main__':
  slave = Slave('localhost', 9999)
  slave.start()