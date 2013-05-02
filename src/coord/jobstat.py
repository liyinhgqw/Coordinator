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
    self.throughput = 0;
    self._backup = -1
    
    self.lock = threading.Lock()
    self.start()
    
  def update_backuprate(self):
    with self.lock:
      cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
      if cur_backup - self._backup > self.backuprate:
        self.backuprate = cur_backup - self._backup

    threading.Timer(self.interval, self.update_backuprate).start()
    
  def update(self, elapse):
    if elapse > 0:
      if self.runtime <= 0:
        self.runtime = elapse
      else:
        self.runtime = self.runtime * 0.6 + elapse * 0.4
        
    with self.lock:
      cur_backup = self.slave.get_unfinished_input_totalsize_wo(self.jobname)
      self.backuprate = cur_backup - self._backup
      self._backup = cur_backup
    
    print 'runtime = ', self.runtime
    print 'backuprate = ', self.backuprate
  
  def update_throughput(self, period):
    if period > 0:
      if self.throughput <= 0:
        self.throughput = period
      else:
        self.throughput = self.throughput * 0.6 + period * 0.4
    print 'throughput = ', self.throughput
    
  def start(self):
    t = threading.Timer(self.interval, self.update_backuprate)
    t.start()
    
  def get_stat(self, stat):
    return self.__dict__[stat]
    
    
    
    
  
  
        