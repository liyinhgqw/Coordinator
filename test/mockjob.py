'''
Mock Job
'''

import sys
import os
import time
import coord.common
from optparse import OptionParser

MSG_USAGE = "usage: %prog [ -f <filename>] [ -m <master>]"

class MockJob(object):
  def __init__(self, jobname, runtime, indir, outdir):
    # jobname is required
    self.jobname = jobname
    self.indir = indir
    self.outdir = outdir
    self.runtime = runtime
    
  def pre_run(self):
    # Tag indir started
    if self.indir is not None:
      os.mkdir(os.path.join(self.indir, self.jobname + coord.common.STARTED_TAG))
      
  def post_run(self):
    # Tag indir finished
    if self.indir is not None:
      os.mkdir(os.path.join(self.indir, self.jobname + coord.common.FINISHED_TAG))  
      
    # Tag job finished
    if self.indir is not None:
      os.mkdir(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + coord.common.FINISHED_TAG))     
      
      
  def runjob(self):
    self.pre_run()
    self.run()
    self.post_run()
    
  def run(self):
    time.sleep(self.runtime)
    
    if self.outdir is not None:
      os.mkdir(self.outdir)
  
    
if __name__ == '__main__':
  optParser = OptionParser(MSG_USAGE)
  
  optParser.add_option("-n",
                       "--name",
                       action = "store",
                       type = "string",
                       dest = "jobname", 
                       help = "Set job name."
                       )
  
  optParser.add_option("-t",
                       "--time",
                       action = "store",
                       type = "float",
                       dest = "runtime", 
                       default = 60.0,
                       help = "Set job duration."
                       )

  optParser.add_option("-i",
                       "--input",
                       action = "store",
                       type = "string",
                       dest = "indir", 
                       help = "Set input directory."
                       ) 
    
  optParser.add_option("-o",
                       "--output",
                       action = "store",
                       type = "string",
                       dest = "outdir", 
                       help = "Set input directory."
                       ) 
  
  options, _ = optParser.parse_args(sys.argv[1:])
  assert options.jobname is not None
  
  print 'start'
    
  
  job = MockJob(options.jobname, options.runtime, options.indir, options.outdir)
  job.runjob()