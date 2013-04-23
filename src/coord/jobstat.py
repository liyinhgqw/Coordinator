'''
Created on Apr 9, 2013

@author: stud
'''

import os
import coord.common
import coord.slave
import threading
from functools import partial

class JobStat(object):
  '''
  Statistics of each job calculated and recorded by slaves.
  '''

  def __init__(self, jobname, interval, slave):
    self.jobname = jobname
    self.interval = interval
    self.slave = slave
    self.lock = threading.Lock()
    
    # avg stats
    self.runtime = -1
    self.backuprate = 0;
    
    self._st = -1
    self._et = -1
    self._elapse = -1
    self._backup = -1
    
    self.start_update()
    
  def has_runtime_stat(self):
    return self.runtime > 0
  
  def start(self):
    with self.lock:
      self._st = coord.common.curtime()
  
  def finish(self):
    if self._st > 0:
      self._et = coord.common.curtime()
      self._elapse = self._et - self._st
      if self._elapse > 0:
        # for runtime
        if self.runtime < 0:
          self.runtime = self._elapse
        else:
          self.runtime = self.runtime * 0.6 + self._elapse * 0.4
        
          cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
          self.backuprate = cur_backup - self._backup
          self._backup = cur_backup
    with self.lock:
      self._st = -1
    
  def update_backuprate(self):
    # for backuprate
      cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
      if cur_backup - self._backup > self.backuprate:
        self.backuprate = cur_backup - self._backup
    
  def update(self):
    # finished
    if self.slave.checknclear_finished_wo(self.jobname):
      self.finish()
      # also tag milestone
      lfs = coord.common.LFS()
      lfs.mkdir(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + 
                                       coord.common.MILESTONE_TAG))
      
      delt = threading.Timer(coord.common.CLEAR_INTERVAL, self.slave.checknclear_milestone_wo)
      delt.start()
      
    self.update_backuprate()
    
    t = threading.Timer(self.interval, self.update)
    t.start()
    
    print 'runtime = ', self.runtime
    print 'backuprate = ', self.backuprate
  
  def start_update(self):
    t = threading.Timer(self.interval, self.update)
    t.start()
    
  def get_stat(self, stat):
    return self.__dict__[stat]
    
    
    
    
  
  
        