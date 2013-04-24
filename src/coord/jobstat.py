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

  def __init__(self, jobname, slave, interval = 1.0):
    self.jobname = jobname
    self.interval = interval
    self.slave = slave
    # avg stats
    self.runtime = -1
    self.backuprate = 0;
    self._elapse = -1
    self._backup = -1
    
    self.start_update()
    
  def do_stat(self):
    try:
      statpath = os.path.join(coord.common.SLAVE_META_PATH, self.jobname
                                 + coord.common.STAT_TAG)
      statfile = open(statpath)
      for stat in statfile:
        self._elapse = float(stat) 
        if self._elapse > 0:
          # for runtime
          if self.runtime < 0:
            self.runtime = self._elapse
          else:
            self.runtime = self.runtime * 0.6 + self._elapse * 0.4
        
      cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
      self.backuprate = cur_backup - self._backup
      self._backup = cur_backup
    except:
      pass
    finally:
      os.unlink(statpath)
      
    
  def check_stat(self):
    lfs = coord.common.LFS()
    return lfs.exists(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + 
                                   coord.common.STAT_TAG))
    
  def update_backuprate(self):
    # for backuprate
      cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
      if cur_backup - self._backup > self.backuprate:
        self.backuprate = cur_backup - self._backup
    
  def update(self):
    # finished
    if self.check_stat():
      self.do_stat()
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
    
    
    
    
  
  
        