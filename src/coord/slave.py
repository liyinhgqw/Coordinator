'''
Created on Apr 1, 2013

@author: liyinhgqw
'''
import time
import logging
import rpc.server
import rpc.client
import coord.common
import coord.jobstat 
import commands
import os
import threading
from functools import partial


class Slave(object):
  class MyHandler(object):
    def __init__(self, slave):
      self.slave = slave
     
    # Called from master
    def heartbeat(self, handle):
      handle.done(True)
      
    # Called from master
    def register_job(self, handle, jobname, jobinfo):
      self.slave.logger.info('register job')
      self.slave.jobmap[jobname] = jobinfo
      self.slave.jobstats[jobname] = coord.jobstat.JobStat(jobname, 1.0, self.slave)
      print jobname, jobinfo
      handle.done(1)
      
    # Called from client
    def cmd(self, handle, cmdstr):
      status, out = commands.getstatusoutput(cmdstr)
      handle.done((status, out))
      
      
    def execute(self, handle, jobname):
      handle.done(True)
      self.slave.execute_wo(jobname)
      
    def delay_execute(self, handle, jobname, timetpl):
      handle.done(True)
      _execute = partial(self.slave.execute_wo, jobname)
      print 'interval', coord.common.delay(timetpl)
      t = threading.Timer(coord.common.delay(timetpl), _execute)
      t.start()
      
    def period_execute(self, handle, jobname, interval, check_finished=False):
      if self.slave._stopjob.has_key(jobname) and self.slave._stopjob[jobname] == True:
        self.slave._stopjob[jobname] = False
      else:
        # do not check finished for the first round
        ret = self.slave.execute_wo(jobname)
        print 'do period execute.', ret
        _period_execute = partial(self.slave.period_execute_wo, jobname, interval)
        t = threading.Timer(interval, _period_execute)
        t.start()
      handle.done(True)
      
    def stop_period_execute(self, handle, jobname):
      self.slave._stopjob[jobname] = True
      handle.done(True)
      
    def get_input_size(self, handle, jobname):
      handle.done(self.slave.get_input_size_wo(jobname))
      
    def get_input_totalsize(self, handle, jobname):
      handle.done(self.slave.get_input_totalsize_wo(jobname))
      
    def get_input_subdirnum(self, handle, jobname):
      handle.done(self.slave.get_input_subdirnum_wo(jobname))
      
    def get_input_subdirtotalnum(self, handle, jobname):
      handle.done(self.slave.get_input_subdirtotalnum_wo(jobname))
      
    def get_unfinished_input_size(self, handle, jobname):
      handle.done(self.slave.get_unfinished_input_size_wo(jobname))
      
    def get_unfinished_input_totalsize(self, handle, jobname):
      handle.done(self.slave.get_unfinished_input_totalsize_wo(jobname))
    
    def get_unfinished_input_subdirnum(self, handle, jobname):
      handle.done(self.slave.get_unfinished_input_subdirnum_wo(jobname))
      
    def get_unfinished_input_subdirtotalnum(self, handle, jobname):
      handle.done(self.slave.get_unfinished_input_subdirtotalnum_wo(jobname))

    def check_finished(self, handle, jobname):
      handle.done(self.slave.check_finished_wo(jobname))
    
    def checknclear_finished(self, handle, jobname):
      handle.done(self.slave.checknclear_finished_wo(jobname))
    
    
    
    
  def __init__(self, host, port):
    self.logger = logging.getLogger("Slave")
    self.logger.setLevel(logging.DEBUG)
    self._port = port + 1  # slave server listen the port+1
    self.rpc_server = rpc.server.RPCServer(coord.common.localhost(), self._port, handler=self.MyHandler(self))
    self.rpc_client = rpc.client.RPCClient(host, port)
    self.jobmap = {}
    self.jobstats = {}
    self._stopjob = {}
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
    
  def get_jobinfo(self, jobname):
    if self.jobmap.has_key(jobname):
      return self.jobmap[jobname]
    else:
      return None
    
  def execute_wo(self, jobname):
    status, out = -1, ''
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      status, out = commands.getstatusoutput(jobinfo['Command'])
      if status == 0:
        self.jobstats[jobname].start()
    print out
    return (status, out)
    
  def period_execute_wo(self, jobname, interval, check_finished=False):
    if self._stopjob.has_key(jobname) and self._stopjob[jobname] == True:
      self._stopjob[jobname] = False
    else:
      if not check_finished or self.checknclear_finished_wo(jobname):
        ret = self.execute_wo(jobname)
        print 'do period execute.', ret
      _period_execute = partial(self.period_execute_wo, jobname, interval)
      t = threading.Timer(interval, _period_execute)
      t.start()
        
  def get_input_size_wo(self, jobname):
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
  
  def get_input_totalsize_wo(self, jobname):
    return sum([dirsize for _, dirsize in self.get_input_size_wo(jobname).iteritems()])

  def get_input_subdirnum_wo(self, jobname):
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

  def get_input_subdirtotalnum_wo(self, jobname):
    return sum([dirnum for _, dirnum in self.get_input_subdirnum_wo(jobname).iteritems()])
        
  def get_unfinished_input_size_wo(self, jobname):
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
  
  def get_unfinished_input_totalsize_wo(self, jobname):
    return sum([dirsize for _, dirsize in self.get_unfinished_input_size_wo(jobname).iteritems()])
      
  def get_unfinished_input_subdirnum_wo(self, jobname):
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
  
  def get_unfinished_input_subdirtotalnum_wo(self, jobname):
    return sum([dirnum for _, dirnum in self.get_unfinished_input_subdirnum_wo(jobname).iteritems()])
       
  def check_finished_wo(self, jobname):
    lfs = coord.common.LFS()
    return lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                         coord.common.FINISHED_TAG))
           
  def checknclear_finished_wo(self, jobname):
    lfs = coord.common.LFS()
    finished = lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                       coord.common.FINISHED_TAG))
    if finished:
      os.remove(os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                       coord.common.FINISHED_TAG))
    return finished
  
if __name__ == '__main__':
  slave = Slave(coord.common.localhost(), coord.common.MASTER_PORT)
  slave.start()
