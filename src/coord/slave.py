'''
Created on Apr 1, 2013

@author: liyinhgqw
'''
import time
import logging
import rpc.server
import rpc.client
import coord.common
import commands
import os
import threading
from functools import partial

class Slave(object):
  class MyHandler(object):
    def __init__(self, slave):
      self.slave = slave
      self._stopjob = {}
      
    def do_something(self, arg1, arg2):
      return int(arg1) + int(arg2)
    
    def foo(self, handle, arg1, arg2):
      handle.done(self.do_something(arg1, arg2))
      
    # Called from master
    def heartbeat(self, handle):
      handle.done(True)
      
    # Called from master
    def register_job(self, handle, jobname, jobinfo):
      self.slave.logger.info('register job')
      self.slave.jobmap[jobname] = jobinfo
      print jobname, jobinfo
      handle.done(1)
      
    def get_jobinfo(self, jobname):
      if self.slave.jobmap.has_key(jobname):
        return self.slave.jobmap[jobname]
      else:
        return None
      
    # Called from client
    def cmd(self, handle, cmdstr):
      status, out = commands.getstatusoutput(cmdstr)
      handle.done((status, out))
      
      
    def _execute(self, jobname):
      status, out = '', ''
      jobinfo = self.get_jobinfo(jobname)
      if jobinfo is not None:
        status, out = commands.getstatusoutput(jobinfo['Command'])
      print out
      return (status, out)
      
    def execute(self, handle, jobname):
      handle.done(self._execute(jobname))
      
    def delay_execute(self, handle, jobname, timetpl):
      handle.done(True)
      _execute = partial(self._execute, jobname)
      print 'interval', coord.common.delay(timetpl)
      t = threading.Timer(coord.common.delay(timetpl), _execute)
      t.start()
      
    def _period_execute(self, jobname, interval, check_finished = False):
      if self._stopjob.has_key(jobname) and self._stopjob[jobname] == True:
        self._stopjob[jobname] = False
      else:
        if not check_finished or self._checknclear_finished(jobname):
          ret = self._execute(jobname)
          print 'do period execute.', ret
        _period_execute = partial(self._period_execute, jobname, interval)
        t = threading.Timer(interval, _period_execute)
        t.start()
      
    def period_execute(self, handle, jobname, interval, check_finished = False):
      if self._stopjob.has_key(jobname) and self._stopjob[jobname] == True:
        self._stopjob[jobname] = False
      else:
        # do not check finished for the first round
        ret = self._execute(jobname)
        print 'do period execute.', ret
        _period_execute = partial(self._period_execute, jobname, interval)
        t = threading.Timer(interval, _period_execute)
        t.start()
      handle.done(True)
      
    def stop_period_execute(self, handle, jobname):
      self._stopjob[jobname] = True
      
    def _get_input_size(self, jobname):
      ret = {}
      jobinfo = self.get_jobinfo(jobname)
      if jobinfo is not None:
        if jobinfo['Inputs'].has_key('LFS'):
          lfs = coord.common.LFS()
          for ldir in jobinfo['Inputs']['LFS']:
            alias, ldirpath = ldir.split('=')
            alias = alias.strip()
            ldirpath = ldirpath.strip()
            ret[alias] = lfs.get_dir_size(ldirpath)
        if jobinfo['Inputs'].has_key('DFS'):
          dfs = coord.common.DFS()
          for ddir in jobinfo['Inputs']['DFS']:
            alias, ddirpath = ddir.split('=')
            alias = alias.strip()
            ddirpath = ddirpath.strip()
            ret[alias] = dfs.get_dir_size(ddirpath)
      return ret
    
    def get_input_size(self, handle, jobname):
      handle.done(self._get_input_size(jobname))
      
    def _get_input_totalsize(self, jobname):
      return sum([dirsize for _, dirsize in self._get_input_size(jobname).iteritems()])
      
    def get_input_totalsize(self, handle, jobname):
      handle.done(self._get_input_totalsize(jobname))
      
    def _get_input_subdirnum(self, jobname):
      ret = {}
      jobinfo = self.get_jobinfo(jobname)
      if jobinfo is not None:
        if jobinfo['Inputs'].has_key('LFS'):
          lfs = coord.common.LFS()
          for ldir in jobinfo['Inputs']['LFS']:
            alias, ldirpath = ldir.split('=')
            alias = alias.strip()
            ldirpath = ldirpath.strip()
            ret[alias] = lfs.get_subdir_num(ldirpath)
        if jobinfo['Inputs'].has_key('DFS'):
          dfs = coord.common.DFS()
          for ddir in jobinfo['Inputs']['DFS']:
            alias, ddirpath = ddir.split('=')
            alias = alias.strip()
            ddirpath = ddirpath.strip()
            ret[alias] = dfs.get_subdir_num(ddirpath)
      return ret
    
    def get_input_subdirnum(self, handle, jobname):
      handle.done(self._get_input_subdirnum(jobname))
      
    def _get_input_subdirtotalnum(self, jobname):
      return sum([dirnum for _, dirnum in self._get_input_subdirnum(jobname).iteritems()])
        
    def get_input_subdirtotalnum(self, handle, jobname):
      handle.done(self._get_input_subdirtotalnum(jobname))
      
      
    def _get_unfinished_input_size(self, jobname):
      ret = {}
      jobinfo = self.get_jobinfo(jobname)
      if jobinfo is not None:
        if jobinfo['Inputs'].has_key('LFS'):
          lfs = coord.common.LFS()
          for ldir in jobinfo['Inputs']['LFS']:
            alias, ldirpath = ldir.split('=')
            alias = alias.strip()
            ldirpath = ldirpath.strip()
            ret[alias] = lfs.get_unfinished_dir_size(ldirpath)
        if jobinfo['Inputs'].has_key('DFS'):
          dfs = coord.common.DFS()
          for ddir in jobinfo['Inputs']['DFS']:
            alias, ddirpath = ddir.split('=')
            alias = alias.strip()
            ddirpath = ddirpath.strip()
            ret[alias] = dfs.get_unfinished_dir_size(ddirpath)
      return ret
    
    def get_unfinished_input_size(self, handle, jobname):
      handle.done(self._get_unfinished_input_size(jobname))
      
    def _get_unfinished_input_totalsize(self, jobname):
      return sum([dirsize for _, dirsize in self._get_unfinished_input_size(jobname).iteritems()])
      
    def get_unfinished_input_totalsize(self, handle, jobname):
      handle.done(self._get_unfinished_input_totalsize(jobname))
      
    def _get_unfinished_input_subdirnum(self, jobname):
      ret = {}
      jobinfo = self.get_jobinfo(jobname)
      if jobinfo is not None:
        if jobinfo['Inputs'].has_key('LFS'):
          lfs = coord.common.LFS()
          for ldir in jobinfo['Inputs']['LFS']:
            alias, ldirpath = ldir.split('=')
            alias = alias.strip()
            ldirpath = ldirpath.strip()
            ret[alias] = lfs.get_unfinished_subdir_num(ldirpath)
        if jobinfo['Inputs'].has_key('DFS'):
          dfs = coord.common.DFS()
          for ddir in jobinfo['Inputs']['DFS']:
            alias, ddirpath = ddir.split('=')
            alias = alias.strip()
            ddirpath = ddirpath.strip()
            ret[alias] = dfs.get_unfinished_subdir_num(ddirpath)
      return ret
    
    def get_unfinished_input_subdirnum(self, handle, jobname):
      handle.done(self._get_unfinished_input_subdirnum(jobname))
      
    def _get_unfinished_input_subdirtotalnum(self, jobname):
      return sum([dirnum for _, dirnum in self._get_unfinished_input_subdirnum(jobname).iteritems()])
        
    def get_unfinished_input_subdirtotalnum(self, handle, jobname):
      handle.done(self._get_unfinished_input_subdirtotalnum(jobname))
      
    def _check_finished(self, jobname):
      lfs = coord.common.LFS()
      return lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                         coord.common.FINISHED_TAG))
    
    def check_finished(self, handle, jobname):
      handle.done(self._check_finished(jobname))
    
    def _checknclear_finished(self, jobname):
      lfs = coord.common.LFS()
      finished = lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                         coord.common.FINISHED_TAG))
      if finished:
        os.remove(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                         coord.common.FINISHED_TAG))
      return finished
    
    def checknclear_finished(self, handle, jobname):
      handle.done(self._checknclear_finished(jobname))
    
  def __init__(self, host, port):
    self.logger = logging.getLogger("Slave")
    self.logger.setLevel(logging.DEBUG)
    self._port = port + 1   # slave server listen the port+1
    self.rpc_server = rpc.server.RPCServer(coord.common.localhost(), self._port, handler=self.MyHandler(self))
    self.rpc_client = rpc.client.RPCClient(host, port)
    self.jobmap = {}
    if (not os.path.exists(coord.common.SLAVE_META_PATH)):
      os.mkdir(coord.common.SLAVE_META_PATH)
    
  def server_forever(self):
    while self.running:
      time.sleep(100)
  
  def start(self):
    self.rpc_server.start()
    register_slave_future = self.rpc_client.register_slave(coord.common.localhost(), int(self._port))
    assert 1 == register_slave_future.wait()

    self.logger.info("Registered slave %s:%d" % (coord.common.localhost(), self._port))
    
    self.running = True
    self.server_forever()
        
  def stop(self):
    ''' Cannot stop immediately '''
    self.running = False
    

if __name__ == '__main__':
  slave = Slave(coord.common.localhost(), coord.common.MASTER_PORT)
  slave.start()