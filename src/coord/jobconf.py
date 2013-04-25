'''
Created on Apr 2, 2013

@author: stud
'''
import sys
import yaml
import coord.common
from optparse import OptionParser
from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

import rpc.client

MSG_USAGE = "usage: %prog [ -f <filename>] [ -m <master>]"
    
class JobConf(object):
  def __init__(self, filename, master):
    self.filename = filename
    self.master = master
  
  def parse_config(self):
    f = open(self.filename, "r")
    for fileinfo in yaml.load_all(f): 
      print 'ok', fileinfo
      self.register_job(fileinfo)
  
  def register_job(self, fileinfo):
    self.rpc_client = rpc.client.RPCClient(self.master, coord.common.MASTER_PORT)
    register_job_future = self.rpc_client.register_job(fileinfo)
    assert 1 == register_job_future.wait()
    
  
if __name__ == '__main__':
    optParser = OptionParser(MSG_USAGE)
    optParser.add_option("-f",
                         "--file",
                         action = "store",
                         type = "string",
                         dest = "fileName", 
                         default = "conf.yaml",
                         help = "Need configuration file name."
                         )

    optParser.add_option("-m",
                         "--master",
                         action = "store",
                         type = "string",
                         dest = "master", 
                         default = "127.0.0.1",
                         help = "Need host:port of master."
                         ) 
    options, _ = optParser.parse_args(sys.argv[1:])
    
    jobConf = JobConf(options.fileName, options.master)
    jobConf.parse_config()
    
    