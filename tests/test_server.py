import time
import rpc.server

class MyHandler(object):
  def do_something(self, arg1, arg2):
    return int(arg1) + int(arg2)
  
  def foo(self, handle, arg1, arg2):
    handle.done(self.do_something(arg1, arg2))
    


if __name__ == '__main__':
  s = rpc.server.RPCServer('127.0.0.1', 9999, handler=MyHandler())
  s.start()
  
  time.sleep(100);