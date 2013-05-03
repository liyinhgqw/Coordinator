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

# input and output has two functions: tag for indirs & provide paths for both indirs and outdirs
#
# Always include the following template in your defined job
#

#import coord.jobtool, coord.common
#import os, sys
#from optparse import OptionParser
#
#class YOUR_JOB(coord.jobtool.JobTool):
#  def run(self):
#    pass
#
#
#MSG_USAGE = "usage: %prog [ -n <jobname>] [ -i <input dir>] [ -o <output dir>] [ -t <runtime>]"
#if __name__ == '__main__':
#  optParser = OptionParser(MSG_USAGE)
#  
#  optParser.add_option("-n",
#                       "--name",
#                       action="store",
#                       type="string",
#                       dest="jobname",
#                       help="Set job name."
#                       )
#  
#
#  optParser.add_option("-i",
#                       "--input",
#                       action="store",
#                       type="string",
#                       dest="indir",
#                       default=None,
#                       help="Set input directory."
#                       ) 
#    
#  optParser.add_option("-o",
#                       "--output",
#                       action="store",
#                       type="string",
#                       dest="outdir",
#                       default=None,
#                       help="Set input directory."
#                       ) 
#  
#  optParser.add_option("-t",
#                       "--time",
#                       action="store",
#                       type="float",
#                       dest="runtime",
#                       default=5.0,
#                       help="Set job duration."
#                       )
#  
#  options, _ = optParser.parse_args(sys.argv[1:])
#  assert options.jobname is not None
#  
#  # only include dirs that need tags
#  job = YOUR_JOB(options.jobname, options.indir, options.outdir, options.runtime)
#  job.runjob()




MSG_USAGE = "usage: %prog [ -n <jobname>] [ -i <input dir>] [ -o <output dir>] [ -t <runtime>]"

class JobTool(object):
  def __init__(self, jobname, indirs, outdirs, runtime=1.0):
    self.jobname = jobname
    if indirs is None or indirs == '':
      self.inputs = {}
    else:
      self.inputs = self.deserialize(indirs)
    if outdirs is None or outdirs == '':
      self.outputs = {}
    else:
      self.outputs = self.deserialize(outdirs)
      
    self.indirs = copy.deepcopy(self.inputs)
    for indir in self.indirs.values():
      indir.path = [indir.path]
    self.outdirs = copy.deepcopy(self.outputs)
    for outdir in self.outdirs.values():
      outdir.path = [outdir.path]
    self.runtime = runtime
    
    self.tmpdir = []
      
  def deserialize(self, dirliststr):
    dirs = {}
    dirstrlist = dirliststr.split(',')
    for dirstr in dirstrlist:
      alias, path, fs, mode = dirstr.split('|')
      dirs[alias] = coord.slave.DirInfo(alias, path, fs, int(mode))
    return dirs
  
  def tag_started(self, fs, path):
    if fs == 'lfs':
      fs = coord.common.LFS()
    elif fs == 'dfs':
      fs = coord.common.DFS()
    else:
      print 'Filesystem not supported'
      fs = None
    
    if fs is not None:  
      fs.mkdir(os.path.join(path, self.jobname + coord.common.STARTED_TAG))

      
  def tag_finished(self, fs, path):
    if fs == 'lfs':
      fs = coord.common.LFS()
    elif fs == 'dfs':
      fs = coord.common.DFS()
    else:
      print 'Filesystem not supported'
      fs = None
    
    if fs is not None:  
      fs.mkdir(os.path.join(path, self.jobname + coord.common.FINISHED_TAG))
      
  def mkdir(self, fs, path):
    fsstr = fs
    if fs == 'lfs':
      fs = coord.common.LFS()
    else:
      exit(1)
      fs = coord.common.DFS()
      
    tmpdir = path + '_tmp'
    try:
      fs.mkdir(tmpdir)
      self.tmpdir.append((fsstr, tmpdir))
    except:
      print 'Cannot make directory.'
      exit(1)
    
    return tmpdir
        
  def pre_run(self, segcheck=True):
    # Tag for decide buffered seg num and make schedule decision
    for ipt in self.inputs.values():
      if ipt.mode == 1:
        nextseg = self.next_unfinished_seg(ipt.fs, ipt.path)
        if segcheck and nextseg is None:
          exit(1)
        self.tag_started(ipt.fs, nextseg)
        print 'input seg (STARTED)=', nextseg
        self.indirs[ipt.alias].path = [nextseg]    # change to seg path
      elif ipt.mode == 2:
        segs = self.unfinished_seg(ipt.fs, ipt.path)
        if segcheck and len(segs) <= 0:
          exit(1)
        for seg in segs:
          self.tag_started(ipt.fs, seg)
        print 'input seg (STARTED)=', segs
        self.indirs[ipt.alias].path = segs    # change to seg path
        
    for opt in self.outputs.values():
      if opt.mode == 1:
        nextseg = self.next_seg(opt.fs, opt.path)
        self.outdirs[opt.alias].path = [nextseg]    # change to seg path
    return True
      
  def post_run(self, segcheck=True):
    # relink tmpdir
    for fs, tmpdir in self.tmpdir:
      if fs == 'lfs':
        fs = coord.common.LFS()
      else:
        fs = coord.common.DFS()
      fs.rename(tmpdir, tmpdir[:-4])
      
    self.tmpdir = []
    
    # Tag for decide next seg to process
    for ipt in self.inputs.values():
      if ipt.mode == 1:
        nextseg = self.next_unfinished_seg(ipt.fs, ipt.path)
        self.tag_finished(ipt.fs, nextseg)
        print 'input seg (FINISHED)=', nextseg
      elif ipt.mode == 2:
        segs = self.unfinished_seg(ipt.fs, ipt.path)
        if segcheck and len(segs) <= 0:
          exit(1)
        for seg in segs:
          self.tag_finished(ipt.fs, seg)
        print 'input seg (FINISHED)=', segs

  def runjob(self, segcheck = True):
    self.pre_run(segcheck)
    self.run()
    print 'RUN COMPLETED.'
    self.post_run(segcheck)
    
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
      lfs.mkdir(pdir)
      segs = [int(seg) for seg in lfs.get_subdirs(pdir) if self.check_valid_seg(seg)]
    elif fs == 'dfs':
      dfs = coord.common.DFS()
      dfs.mkdir(pdir)
      segs = [int(seg) for seg in dfs.get_subdirs(pdir) if self.check_valid_seg(seg)]

    return  os.path.join(pdir, '0') if len(segs) <=0 else os.path.join(pdir, str(max(segs) + 1))
      
      
  def next_unfinished_seg(self, fs, pdir):
    if fs == 'lfs':
      fs = coord.common.LFS()
    elif fs == 'dfs':
      fs = coord.common.DFS()

    segs = [int(seg) for seg in fs.get_unfinished_subdirs(pdir, self.jobname) if self.check_valid_seg(seg)]
    return None if len(segs) <=0 else os.path.join(pdir, str(min(segs)))
  
  def unfinished_seg(self, fs, pdir):
    if fs == 'lfs':
      fs = coord.common.LFS()
    elif fs == 'dfs':
      fs = coord.common.DFS()

    segs = [int(seg) for seg in fs.get_unfinished_subdirs(pdir, self.jobname) if self.check_valid_seg(seg)]
    return map(lambda seg: os.path.join(pdir, str(seg)), segs)
  
  # not necessary
  def touch_dir(self, fs, dirname):
    if fs == 'lfs':
      fs = coord.common.LFS()
    else:
      fs = coord.common.DFS()
    fs.mkdir(dirname)
  
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
