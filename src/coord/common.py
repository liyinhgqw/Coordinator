'''
Created on Apr 2, 2013

@author: stud
'''

import socket

def _getsockname():
  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  port = socket.getservbyname("http", "tcp")
  s.connect(("www.google.com", port))
  sockname = s.getsockname()
  s.close()
  return sockname
  
def localhost():
  return _getsockname()[0]
