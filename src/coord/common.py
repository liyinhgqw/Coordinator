'''
Created on Apr 2, 2013

@author: stud
'''

import os
import socket
import time, datetime
from os.path import join, getsize

MASTER_PORT = 9999
SLAVE_PORT = MASTER_PORT + 1
FINISHED_TAG = '_FINISHED'
STARTED_TAG = '_STARTED'
DONE_TAG = 'DONE'
SLAVE_META_PATH = '/tmp/coord'
CLEAR_INTERVAL = 20
RETRY_INTERVAL = 2

def _getsockname():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  port = socket.getservbyname("http", "tcp")
  s.connect(("www.google.com", port))
  sockname = s.getsockname()
  s.close()
  return sockname
  
def localhost():
  return _getsockname()[0]

class LFS(object):
  def __init__(self):
    pass
  
  def mkdir(self, dirname):
    if not os.path.exists(dirname):
      os.mkdir(dirname)
  
  def rmdir(self, dirname):
    if os.path.exists(dirname):
      os.rmdir(dirname)
      
  def rm_rf(self, d):
    if os.path.exists(d):
      for path in (os.path.join(d,f) for f in os.listdir(d)):
        if os.path.isdir(path):
          self.rm_rf(path)
        else:
          os.unlink(path)
      os.rmdir(d)
    
  def is_done(self, dirname):
    return os.path.exists(os.path.join(dirname, DONE_TAG))
  
  def get_subdirs(self, dirname, checkdone = False):
    return [sdir for sdir in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, sdir)) 
            if not checkdone or self.is_done(os.path.join(dirname, sdir))]
  
  def get_unfinished_subdirs(self, dirname, jobname = '', checkdone = False):
    return [sdir for sdir in self.get_subdir_num(dirname, checkdone) and
               not os.path.exists(os.path.join(dirname, sdir, jobname + FINISHED_TAG))]
    
  def get_buffered_subdirs(self, dirname, jobname = '', checkdone = False):
    return [sdir for sdir in self.get_subdir_num(dirname, checkdone = False) and
               not os.path.exists(os.path.join(dirname, sdir, jobname + FINISHED_TAG))
               and not os.path.exists(os.path.join(dirname, sdir, jobname + STARTED_TAG))]
    
  def get_subfiles(self, dirname):
    return [sfile for sfile in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, sfile)) ]
  
  def get_subdir_num(self, dirname):
    return len(self.get_subdirs(dirname))
  
  def get_unfinished_subdir_num(self, dirname, jobname = '', checkdone = False):
    return len(self.get_unfinished_subdirs(dirname, jobname, checkdone = False))
  
  def get_buffered_subdir_num(self, dirname, jobname = '', checkdone = False):
    return len(self.get_buffered_subdirs(dirname, jobname, checkdone = False))
  
  def get_subfile_num(self, dirname):
    return len(self.get_subfiles(dirname))
  
  # recursive
  def get_dir_size(self, dirname):
    size = 0L
    for root, dirs, files in os.walk(dirname):
      size += sum([getsize(join(root, name)) for name in files])
    return size
  
  # recursive
  def get_unfinished_dir_size(self, dirname, jobname = ''):
    size = 0L
    for root, dirs, files in os.walk(dirname):
      if not (jobname + FINISHED_TAG) in dirs: 
        size += sum([getsize(join(root, name)) for name in files])
    return size
  
  def get_buffered_dir_size(self, dirname, jobname = ''):
    size = 0L
    for root, dirs, files in os.walk(dirname):
      if (not (jobname + FINISHED_TAG) in dirs) and (not (jobname + STARTED_TAG) in dirs): 
        size += sum([getsize(join(root, name)) for name in files])
    return size
  
  def exists(self, pathname):
    return os.path.exists(pathname)
  
  
class DFS(object):
  def __init__(self):
    pass
  
def gettime(timetpl):
  return time.mktime(datetime.datetime(*timetpl).timetuple()) 
    
def curtime():
  return time.time()
    
def delay(timetpl):
  return gettime(timetpl) - curtime()
  