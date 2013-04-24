'''
Created on Apr 1, 2013

@author: liyinhgqw
'''
import time
import psutil
import logging
import rpc.server
import rpc.client
import coord.common
import coord.jobstat 
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
      self.slave.jobstats[jobname] = coord.jobstat.JobStat(jobname, self.slave)
      print jobname, jobinfo
      handle.done(1)
      
    # Called from client
    def cmd(self, handle, jobname, cmdstr):
      status = os.system(cmdstr + " &")
      handle.done(status)
      
      
    def execute(self, handle, jobname, check_finished=True):
      runnable = self.isrunnable(jobname, check_finished)
      if runnable:
        self.slave.execute_wo(jobname, check_finished)
      handle.done(runnable)
      
    def delay_execute(self, handle, jobname, timetpl, check_finished=True):
      handle.done(True)
      _execute = partial(self.slave.execute_wo, jobname, check_finished)
      print 'interval', coord.common.delay(timetpl)
      t = threading.Timer(coord.common.delay(timetpl), _execute)
      t.start()
      
    def period_execute(self, handle, jobname, interval=1.0, check_finished=True):
      self.slave._stopjob[jobname] = False
      # do not check finished for the first round
      ret = self.slave.execute_wo(jobname)
      print 'do period execute.', ret
      _period_execute = partial(self.slave.period_execute_wo, jobname, interval, check_finished)
      t = threading.Timer(interval, _period_execute)
      t.start()
      handle.done(True)
      
    def stop_period_execute(self, handle, jobname):
      self.slave._stopjob[jobname] = True
      handle.done(True)
    
    # input
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

    def get_buffered_input_size(self, handle, jobname):
      handle.done(self.slave.get_buffered_input_size_wo(jobname))
      
    def get_buffered_input_totalsize(self, handle, jobname):
      handle.done(self.slave.get_buffered_input_totalsize_wo(jobname))
    
    def get_buffered_input_subdirnum(self, handle, jobname):
      handle.done(self.slave.get_buffered_input_subdirnum_wo(jobname))
      
    def get_buffered_input_subdirtotalnum(self, handle, jobname):
      handle.done(self.slave.get_buffered_input_subdirtotalnum_wo(jobname))

    # output
    def get_output_size(self, handle, jobname):
      handle.done(self.slave.get_output_size_wo(jobname))
      
    def get_output_totalsize(self, handle, jobname):
      handle.done(self.slave.get_output_totalsize_wo(jobname))
      
    def get_output_subdirnum(self, handle, jobname):
      handle.done(self.slave.get_output_subdirnum_wo(jobname))
      
    def get_output_subdirtotalnum(self, handle, jobname):
      handle.done(self.slave.get_output_subdirtotalnum_wo(jobname))
      
    def get_unfinished_output_size(self, handle, jobname, depjob):
      handle.done(self.slave.get_unfinished_output_size_wo(jobname, depjob))
      
    def get_unfinished_output_totalsize(self, handle, jobname, depjob):
      handle.done(self.slave.get_unfinished_output_totalsize_wo(jobname, depjob))
    
    def get_unfinished_output_subdirnum(self, handle, jobname, depjob):
      handle.done(self.slave.get_unfinished_output_subdirnum_wo(jobname, depjob))
      
    def get_unfinished_output_subdirtotalnum(self, handle, jobname, depjob):
      handle.done(self.slave.get_unfinished_output_subdirtotalnum_wo(jobname, depjob))

    def get_buffered_output_size(self, handle, jobname, depjob):
      handle.done(self.slave.get_buffered_output_size_wo(jobname, depjob))
      
    def get_buffered_output_totalsize(self, handle, jobname, depjob):
      handle.done(self.slave.get_buffered_output_totalsize_wo(jobname, depjob))
    
    def get_buffered_output_subdirnum(self, handle, jobname, depjob):
      handle.done(self.slave.get_buffered_output_subdirnum_wo(jobname, depjob))
      
    def get_buffered_output_subdirtotalnum(self, handle, jobname, depjob):
      handle.done(self.slave.get_buffered_output_subdirtotalnum_wo(jobname, depjob))
      
    # job finish
    def check_finished(self, handle, jobname):
      handle.done(self.slave.check_finished_wo(jobname))
    
    def check_milestone(self, handle, jobname):
      handle.done(self.slave.check_milestone_wo(jobname))
      
#    def checknclear_milestone(self, handle, jobname):
#      handle.done(self.slave.checknclear_milestone_wo(jobname))
    
    def has_input(self, handle, jobname, threshold = 1000000):
      handle.done(self.slave.get_unfinished_input_size_wo(jobname) > threshold)
      
    def has_output(self, handle, jobname, depjob, threshold = 1000000):
      handle.done(self.slave.get_unfinished_output_size_wo(jobname, depjob) > threshold)
    
    def get_stat(self, handle, jobname, stat):
      handle.done(self.slave.get_stat_wo(jobname, stat))

    def get_slave_stat(self, handle):
      handle.done(self.slave.get_slave_stat_wo())



    
  def __init__(self, host, port, recovery = False):
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
    self.recovery = recovery
    
  def server_forever(self):
    while self.running:
      time.sleep(100)
  
  def start(self):
    self.rpc_server.start()
    register_slave_future = self.rpc_client.register_slave(coord.common.localhost(), int(self._port))
    assert 1 == register_slave_future.wait()

    self.logger.info("Registered slave %s:%d" % (coord.common.localhost(), self._port))
    
    self.running = True
    if self.recovery: 
      self.recover()
    self.server_forever()
        
  def stop(self):
    ''' Cannot stop immediately '''
    self.running = False
    
  def find_job_from_tag(self, tag):
    index = tag.find('_')
    if index >= 0:
      return tag[:index]
    
  def recover(self):
    # lookat meta dir, and recover jobs that have STARTED_TAG
    jobs = [dirname for dirname in os.listdir(coord.common.SLAVE_META_PATH) 
            if os.path.isdir(dirname) and dirname.endswith(coord.common.STARTED_TAG)]
    jobs = map(self.find_job_from_tag, jobs)
    for job in jobs:
      self.execute_wo(job, False)
    
  def get_jobinfo(self, jobname):
    if self.jobmap.has_key(jobname):
      return self.jobmap[jobname]
    else:
      return None
    
  def isrunnable(self, jobname, check_finished = True):
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is None:
      return False
    
    if not check_finished or self.check_finished_wo(jobname):   # only clean finish tag in jobstat
      return True
    else:
      return False
    
  def execute_wo(self, jobname, check_finished=True):
    print 'execute:', jobname
    jobinfo = self.get_jobinfo(jobname)
    status = os.system(jobinfo['Command'] + " &")
    return status == 0
    
  def period_execute_wo(self, jobname, interval, check_finished=True):
    if self._stopjob.has_key(jobname) and self._stopjob[jobname] == True:
      self._stopjob[jobname] = False
    else:
      status = self.execute_wo(jobname, check_finished)
      print 'do period execute.', status
      _period_execute = partial(self.period_execute_wo, jobname, interval)
      t = threading.Timer(interval, _period_execute)
      t.start()

  # Input Stats        
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
          ret[alias] = lfs.get_unfinished_dir_size(ldirpath, jobname)
      if jobinfo['Inputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Inputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_unfinished_dir_size(ddirpath, jobname)
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
          ret[alias] = lfs.get_unfinished_subdir_num(ldirpath, jobname)
      if jobinfo['Inputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Inputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_unfinished_subdir_num(ddirpath, jobname)
    return ret
  
  def get_unfinished_input_subdirtotalnum_wo(self, jobname):
    return sum([dirnum for _, dirnum in self.get_unfinished_input_subdirnum_wo(jobname).iteritems()])

  def get_buffered_input_size_wo(self, jobname):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Inputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Inputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_buffered_dir_size(ldirpath, jobname)
      if jobinfo['Inputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Inputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_buffered_dir_size(ddirpath, jobname)
    return ret
  
  def get_buffered_input_totalsize_wo(self, jobname):
    return sum([dirsize for _, dirsize in self.get_buffered_input_size_wo(jobname).iteritems()])
      
  def get_buffered_input_subdirnum_wo(self, jobname):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Inputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Inputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_buffered_subdir_num(ldirpath, jobname)
      if jobinfo['Inputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Inputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_buffered_subdir_num(ddirpath, jobname)
    return ret
  
  def get_buffered_input_subdirtotalnum_wo(self, jobname):
    return sum([dirnum for _, dirnum in self.get_buffered_input_subdirnum_wo(jobname).iteritems()])


  # Output Stats        
  def get_output_size_wo(self, jobname):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_dir_size(ldirpath)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_dir_size(ddirpath)
    return ret        
  
  def get_output_totalsize_wo(self, jobname):
    return sum([dirsize for _, dirsize in self.get_output_size_wo(jobname).iteritems()])

  def get_output_subdirnum_wo(self, jobname):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_subdir_num(ldirpath)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_subdir_num(ddirpath)
    return ret

  def get_output_subdirtotalnum_wo(self, jobname):
    return sum([dirnum for _, dirnum in self.get_output_subdirnum_wo(jobname).iteritems()])
        
  def get_unfinished_output_size_wo(self, jobname, depjob):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_unfinished_dir_size(ldirpath, depjob)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_unfinished_dir_size(ddirpath, depjob)
    return ret
  
  def get_unfinished_output_totalsize_wo(self, jobname, depjob):
    return sum([dirsize for _, dirsize in self.get_unfinished_output_size_wo(jobname, depjob).iteritems()])
      
  def get_unfinished_output_subdirnum_wo(self, jobname, depjob):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_unfinished_subdir_num(ldirpath, depjob)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_unfinished_subdir_num(ddirpath, depjob)
    return ret
  
  def get_unfinished_output_subdirtotalnum_wo(self, jobname, depjob):
    return sum([dirnum for _, dirnum in self.get_unfinished_output_subdirnum_wo(jobname, depjob).iteritems()])

  def get_buffered_output_size_wo(self, jobname, depjob):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_buffered_dir_size(ldirpath, depjob)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_buffered_dir_size(ddirpath, depjob)
    return ret
  
  def get_buffered_output_totalsize_wo(self, jobname, depjob):
    return sum([dirsize for _, dirsize in self.get_buffered_output_size_wo(jobname, depjob).iteritems()])
      
  def get_buffered_output_subdirnum_wo(self, jobname, depjob):
    ret = {}
    jobinfo = self.get_jobinfo(jobname)
    if jobinfo is not None:
      if jobinfo['Outputs'].has_key('LFS'):
        lfs = coord.common.LFS()
        for ldir in jobinfo['Outputs']['LFS']:
          alias, ldirpath = ldir.split('=')
          alias = alias.strip()
          ldirpath = ldirpath.strip()
          ret[alias] = lfs.get_buffered_subdir_num(ldirpath, depjob)
      if jobinfo['Outputs'].has_key('DFS'):
        dfs = coord.common.DFS()
        for ddir in jobinfo['Outputs']['DFS']:
          alias, ddirpath = ddir.split('=')
          alias = alias.strip()
          ddirpath = ddirpath.strip()
          ret[alias] = dfs.get_buffered_subdir_num(ddirpath, depjob)
    return ret
  
  def get_buffered_output_subdirtotalnum_wo(self, jobname, depjob):
    return sum([dirnum for _, dirnum in self.get_buffered_output_subdirnum_wo(jobname, depjob).iteritems()])

  # job finish
  def check_finished_wo(self, jobname):
    lfs = coord.common.LFS()
    return not lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + 
                                         coord.common.STARTED_TAG))
  
  def check_milestone_wo(self, jobname):
    lfs = coord.common.LFS()
    lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, jobname + 
                                       coord.common.MILESTONE_TAG))
      
#  def checknclear_milestone_wo(self, jobname):
#    milestone = self.check_milestone_wo(jobname)
#    if milestone:
#      lfs = coord.common.LFS()
#      lfs.rmdir(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + 
#                                         coord.common.MILESTONE_TAG))
#    return milestone
    
  def get_stat_wo(self, jobname, stat):
    if self.jobstats.has_key(jobname):
      return self.jobstats[jobname].get_stat(stat)
  
  def get_slave_stat_wo(self):
    stat = {}
    stat['cpu'] = psutil.cpu_percent(0.5)
    return stat
    
if __name__ == '__main__':
  slave = Slave(coord.common.localhost(), coord.common.MASTER_PORT)
  slave.start()
