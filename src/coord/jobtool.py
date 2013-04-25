'''
Inherit this JobTool to implement your jobs
'''

import sys
import os
import time
import coord.common
import coord.slave
import copy
from optparse import OptionParser

# TODO: tmp link

MSG_USAGE = "usage: %prog [ -n <jobname>] [ -i <input dir>] [ -o <output dir>] [ -t <runtime>]"

class JobTool(object):
  def __init__(self, jobname, indirs, outdirs, runtime=1.0):
    self.jobname = jobname
    if indirs is not None:
      self.inputs = self.deserialize(indirs)
    if outdirs is not None:
      self.outputs = self.deserialize(outdirs)
      
    self.runtime = runtime
      
  def deserialize(self, dirliststr):
    dirs = {}
    dirstrlist = dirliststr.split(',')
    for dirstr in dirstrlist:
      alias, path, fs, mode = dirstr.split('|')
      dirs[alias] = coord.slave.DirInfo(alias, path, fs, int(mode))
    return dirs
  
  def tag_started(self, fs, path):
    if fs == 'lfs':
      lfs = coord.common.LFS()
      lfs.mkdir(os.path.join(path, self.jobname + coord.common.STARTED_TAG))
    elif fs == 'dfs':
      dfs = coord.common.DFS()
      dfs.mkdir(os.path.join(path, self.jobname + coord.common.STARTED_TAG))      
    else:
      print 'Filesystem not supported'
      
  def tag_finished(self, fs, path):
    if fs == 'lfs':
      lfs = coord.common.LFS()
      lfs.mkdir(os.path.join(path, self.jobname + coord.common.FINISHED_TAG))
    elif fs == 'dfs':
      dfs = coord.common.DFS()
      dfs.mkdir(os.path.join(path, self.jobname + coord.common.FINISHED_TAG))      
    else:
      print 'Filesystem not supported'
       
  def pre_run(self):
    # Tag for decide buffered seg num and make schedule decision
    for ipt in self.inputs.values():
      if ipt.mode == 1:
        nextseg = self.next_unfinished_seg(ipt.fs, ipt.path)
        if nextseg is None:
          exit(1)
        self.tag_started(ipt.fs, nextseg)
        print 'input seg (STARTED)=', nextseg
    return True
      
  def post_run(self):
    # Tag for decide next seg to process
    for ipt in self.inputs.values():
      if ipt.mode == 1:
        nextseg = self.next_unfinished_seg(ipt.fs, ipt.path)
        self.tag_finished(ipt.fs, nextseg)
        print 'input seg (FINISHED)=', nextseg,

  def runjob(self):
    self.pre_run()
    self.run()
    self.post_run()
    
  def run(self):
    print 'runtime = ', self.runtime
    time.sleep(self.runtime)
    print self.inputs['I1'].fs, self.inputs['I1'].path
    print 'finished'
    
  def check_valid_seg(self, seg):
    try:
      segnum = int(seg)
      if segnum >= 0:
        return True
      else:
        return False
    except:
      return False
    
  def next_seg(self, fs, pdir):
    if fs == 'lfs':
      lfs = coord.common.LFS()
      segs = [int(seg) for seg in lfs.get_subdirs(pdir) if self.check_valid_seg(seg)]
    elif fs == 'dfs':
      dfs = coord.common.DFS()
      segs = [int(seg) for seg in dfs.get_subdirs(pdir) if self.check_valid_seg(seg)]

    return  os.path.join(pdir, '0') if len(segs) <=0 else os.path.join(pdir, str(max(segs) + 1))
      
      
  def next_unfinished_seg(self, fs, pdir):
    if fs == 'lfs':
      lfs = coord.common.LFS()
      segs = [int(seg) for seg in lfs.get_unfinished_subdirs(pdir, self.jobname) if self.check_valid_seg(seg)]
    elif fs == 'dfs':
      dfs = coord.common.DFS()
      segs = [int(seg) for seg in dfs.get_unfinished_subdirs(pdir, self.jobname) if self.check_valid_seg(seg)]

    return None if len(segs) <=0 else os.path.join(pdir, str(min(segs)))
  
  # not necessary
  def touch_dir(self, fs, dirname):
    if fs == 'lfs':
      lfs = coord.common.LFS()
      lfs.mkdir(dirname)
    else:
      dfs = coord.common.DFS()
      dfs.mkdir(dirname)
  
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
                       default=None,
                       help="Set input directory."
                       ) 
    
  optParser.add_option("-o",
                       "--output",
                       action="store",
                       type="string",
                       dest="outdir",
                       default=None,
                       help="Set input directory."
                       ) 
  
  optParser.add_option("-t",
                       "--time",
                       action="store",
                       type="float",
                       dest="runtime",
                       default=5.0,
                       help="Set job duration."
                       )
  
  options, _ = optParser.parse_args(sys.argv[1:])
  assert options.jobname is not None
  
  # only include dirs that need tags
  job = JobTool(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob()
