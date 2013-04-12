'''
Mock Job
'''

import sys
import os
import time
import coord.common
from optparse import OptionParser

MSG_USAGE = "usage: %prog [ -n <jobname>] [ -t <runtime>] [ -i <input dir>] [ -o <output dir>]"

class MockJob(object):
  def __init__(self, jobname, runtime, indir, outdir):
    print 'start'
    # jobname is required
    self.jobname = jobname
    self.indir = indir
    self.outdir = outdir
    self.runtime = runtime
    
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
    
    if self.outdir is not None:
      os.mkdir(self.outdir)
  
def check_valid_seg(seg):
  try:
    segnum = int(seg)
    return True
  except:
    return False
  
def find_next_seg(pdir):
  lfs = coord.common.LFS()
  segs = [int(seg) for seg in lfs.get_subdirs(pdir) if check_valid_seg(seg)]
  if len(segs) <= 0:
    return os.path.join(pdir, '0')
  return os.path.join(pdir, str(max(segs) + 1))

def find_next_buffered_seg(jobname, pdir):
  lfs = coord.common.LFS()
  segs = [int(seg) for seg in lfs.get_buffered_subdirs(pdir, jobname) if check_valid_seg(seg)]
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
                       "--batch",
                       action = "store_true",
                       dest = "batch",
                       default = False, 
                       help = "Set batch mode."
                       ) 

  optParser.add_option("-B",
                       "--outbatch",
                       action = "store_true",
                       dest = "outbatch",
                       default = False, 
                       help = "Set outbatch mode."
                       ) 
  
  options, _ = optParser.parse_args(sys.argv[1:])
  assert options.jobname is not None
  
  if options.batch:
    assert options.indir is not None
    indir = find_next_buffered_seg(options.jobname, options.indir)
    if indir is None:
      sys.exit()
  elif options.outbatch:
    indir = None
  else:
    indir = options.indir
    
  if options.batch or options.outbatch:
    assert options.outdir is not None
    outdir = find_next_seg(options.outdir) 
  else:
    outdir = options.outdir
  
  print 'indir:', indir
  print 'outdir:', outdir
  
  job = MockJob(options.jobname, options.runtime, indir, outdir)
  job.runjob()
