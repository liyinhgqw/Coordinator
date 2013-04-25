'''
Inherit this JobTool to implement your jobs
'''

import sys
import os
import time
import coord.common
from optparse import OptionParser

MSG_USAGE = "usage: %prog [ -n <jobname>] [ -i <input dir>] [ -o <output dir>] \
               [ -b <inbatch>] [ -B <outbatch>] [ -t <runtime>]"

class JobTool(object):
  def __init__(self, jobname, indir, outdir, runtime=1.0):
    # jobname is required
    self.jobname = jobname
    if indir is not None:
      self.indir = indir.split(',')
    else:
      self.indir = None
    if outdir is not None:
      self.outdir = outdir.split(',')
    else:
      self.outdir = None
    self.runtime = runtime
    
    if self.outdir is not None:
      lfs = coord.common.LFS()
      map(lfs.mkdir, self.outdir)
      
  def pre_run(self):
    lfs = coord.common.LFS()
    # Tag indir started
    if self.indir is not None:
      for dirname in lfs.get_subdirs(self.indir, False):
        lfs.mkdir(os.path.join(dirname, self.jobname + coord.common.STARTED_TAG))
      
  def post_run(self):
    lfs = coord.common.LFS()
    for ipt in self.inputs:
      if ipt.mode == 1:
        nextseg = self.next_seg(ipt)
        if ipt.fs == 'lfs':
          lfs.mkdir(os.path.join(nextseg, self.jobname + coord.common.FINISHED_TAG))
        else:
          dfs = coord.common.DFS()
          dfs.mkdir(os.path.join(nextseg, self.jobname + coord.common.FINISHED_TAG))

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
  
  # TODO: add ouput job finished tag
  def find_next_buffered_seg(self, pdir):
    lfs = coord.common.LFS()
    segs = [int(seg) for seg in lfs.get_buffered_subdirs(pdir, self.jobname) if self.check_valid_seg(seg)]
    if len(segs) <= 0:
      return None
    return os.path.join(pdir, str(min(segs)))
  
  
  def touch_dir(self, dirname):
    lfs = coord.common.LFS()
    lfs.mkdir(dirname)
  
if __name__ == '__main__':
  optParser = OptionParser(MSG_USAGE)
  
  optParser.add_option("-n",
                       "--name",
                       action="store",
                       type="string",
                       dest="jobname",
                       help="Set job name."
                       )
  

  optParser.add_option("-i",
                       "--input",
                       action="store",
                       type="string",
                       dest="indir",
                       help="Set input directory."
                       ) 
    
  optParser.add_option("-o",
                       "--output",
                       action="store",
                       type="string",
                       dest="outdir",
                       help="Set input directory."
                       ) 
  
  optParser.add_option("-t",
                       "--time",
                       action="store",
                       type="float",
                       dest="runtime",
                       default=60.0,
                       help="Set job duration."
                       )
  
  options, _ = optParser.parse_args(sys.argv[1:])
  assert options.jobname is not None
  
  # only include dirs that need tags
  job = JobTool(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob()
