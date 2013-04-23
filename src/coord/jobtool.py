'''
Inherit this JobTool to implement your jobs
'''

import sys
import os
import time
import coord.common
from optparse import OptionParser

MSG_USAGE = "usage: %prog [ -n <jobname>] [ -t <runtime>] [ -i <input dir>] [ -o <output dir>]"

class JobTool(object):
  def __init__(self, jobname, indir, outdir, inbatch = False, outbatch = False, runtime = 1.0):
    print 'start'
    # jobname is required
    self.jobname = jobname
    self.indir = indir
    self.outdir = outdir
    self.inbatch = inbatch
    self.outbatch = outbatch
    self.runtime = runtime
    
    if self.outdir is not None:
      os.mkdir(self.outdir)
      
    if self.inbatch:
      assert self.indir is not None
      self.indir = self.find_next_buffered_seg(jobname, self.indir)
    
    if self.outbatch:
      outdir = self.find_next_seg(self.outdir) 
      if outdir is not None:
        os.mkdir(self.outdir)
    
  def pre_run(self):
    # Tag indir started
    if self.indir is not None and not os.path.exists(os.path.join(self.indir, self.jobname + coord.common.STARTED_TAG)):
      os.mkdir(os.path.join(self.indir, self.jobname + coord.common.STARTED_TAG))
      
  def post_run(self):
    # Tag indir finished
    if self.indir is not None and not os.path.exists(os.path.join(self.indir, self.jobname + coord.common.FINISHED_TAG)):
      os.mkdir(os.path.join(self.indir, self.jobname + coord.common.FINISHED_TAG))  
      
    # Tag job finished
    if self.indir is not None and not os.path.exists(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + coord.common.FINISHED_TAG)):
      os.mkdir(os.path.join(coord.common.SLAVE_META_PATH, self.jobname + coord.common.FINISHED_TAG))   
      
      
  def runjob(self):
    self.pre_run()
    self.run()
    self.post_run()
    
  def run(self):
    time.sleep(self.runtime)
    
  def check_valid_seg(self, seg):
    try:
      segnum = int(seg)
      if segnum >= 0:
        return True
      else:
        return False
    except:
      return False
    
  def find_next_seg(self, pdir):
    lfs = coord.common.LFS()
    segs = [int(seg) for seg in lfs.get_subdirs(pdir) if self.check_valid_seg(seg)]
    if len(segs) <= 0:
      return os.path.join(pdir, '0')
    return os.path.join(pdir, str(max(segs) + 1))
  
  def find_next_buffered_seg(self, jobname, pdir):
    lfs = coord.common.LFS()
    segs = [int(seg) for seg in lfs.get_buffered_subdirs(pdir, jobname) if self.check_valid_seg(seg)]
    if len(segs) <= 0:
      return None
    return os.path.join(pdir, str(min(segs)))

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
  
  optParser.add_option("-b",
                       "--inbatch",
                       action = "store_true",
                       dest = "inbatch",
                       default = False, 
                       help = "Set input batch mode."
                       ) 

  optParser.add_option("-B",
                       "--outbatch",
                       action = "store_true",
                       dest = "outbatch",
                       default = False, 
                       help = "Set output batch mode."
                       ) 
  
  options, _ = optParser.parse_args(sys.argv[1:])
  assert options.jobname is not None
  
  job = JobTool(options.jobname, options.runtime, options.indir, options.outdir, options.inbatch, options.outbatch)
  job.runjob()
