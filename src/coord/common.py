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

def _getsockname():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  port = socket.getservbyname("http", "tcp")
  s.connect(("www.google.com", port))
  sockname = s.getsockname()
  s.close()
  return sockname
  
def localhost():
  return _getsockname()[0]

def get_subdirs(dirname):
  return [sdir for sdir in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, sdir)) ]

def get_unfinished_subdirs(dirname):
  return [sdir for sdir in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, sdir)) and
             not os.path.exists(os.path.join(dirname, sdir, FINISHED_TAG))]
  
def get_subfiles(dirname):
  return [sfile for sfile in os.listdir(dirname) if os.path.isfile(os.path.join(dirname, sfile)) ]

def get_subdir_num(dirname):
  return len(get_subdirs(dirname))

def get_unfinished_subdir_num(dirname):
  return len(get_unfinished_subdirs(dirname))

def get_subfile_num(dirname):
  return len(get_subfiles(dirname))

# recursive
def get_dir_size(dirname):
  size = 0L
  for root, dirs, files in os.walk(dirname):
    size += sum([getsize(join(root, name)) for name in files])
  return size

# recursive
def get_unfinished_dir_size(dirname):
  size = 0L
  for root, dirs, files in os.walk(dirname):
    if not FINISHED_TAG in files: 
      size += sum([getsize(join(root, name)) for name in files])
  return size

