import coord.jobtool, coord.common
import os, sys
import random, time
from optparse import OptionParser

# I1 = '/home/stud/workspace/Coordinator/example/adwork/select ='
# O1 = '/home/stud/workspace/Coordinator/example/adwork/identity =' 

class Identity(coord.jobtool.JobTool):
  def run(self):
    lfs = coord.common.LFS()
    adlist = lfs.get_abs_subfiles(self.indirs['I1'].path[0])
    destdir = self.outdirs['O1'].path[0]
    destdir = self.mkdir('lfs', destdir)
    destfile = open(os.path.join(destdir, 'ads'), 'w')
    for adfile in adlist:
      for ad in open(adfile, 'r'):
#        time.sleep(0.02)
        destfile.write(ad)
    destfile.flush()
    destfile.close()
    
MSG_USAGE = "usage: %prog [ -n <jobname>] [ -i <input dir>] [ -o <output dir>] [ -t <runtime>]"
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
  job = Identity(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob()