'''
Created on Apr 2, 2013

@author: stud
'''

import os
import socket
from os.path import join, getsize

MASTER_PORT = 9999
SLAVE_PORT = MASTER_PORT + 1
FINISHED_TAG = 'FINISHED'
SLAVE_META_PATH = '/tmp/coord'

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
  
  def get_subdirs(self, dirname):
    return [sdir for sdir in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, sdir)) ]
  
  def get_unfinished_subdirs(self, dirname):
    return [sdir for sdir in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, sdir)) and
               not os.path.exists(os.path.join(dirname, sdir, FINISHED_TAG))]
    
  def get_subfiles(self, dirname):
    return [sfile for sfile in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, sfile)) ]
  
  def get_subdir_num(self, dirname):
    return len(self.get_subdirs(dirname))
  
  def get_unfinished_subdir_num(self, dirname):
    return len(self.get_unfinished_subdirs(dirname))
  
  def get_subfile_num(self, dirname):
    return len(self.get_subfiles(dirname))
  
  # recursive
  def get_dir_size(self, dirname):
    size = 0L
    for root, dirs, files in os.walk(dirname):
      size += sum([getsize(join(root, name)) for name in files])
    return size
  
  # recursive
  def get_unfinished_dir_size(self, dirname):
    size = 0L
    for root, dirs, files in os.walk(dirname):
      if not FINISHED_TAG in files: 
        size += sum([getsize(join(root, name)) for name in files])
    return size
  
  def exists(self, pathname):
    return os.path.exists(pathname)

class DFS(object):
  def __init__(self):
    pass
  