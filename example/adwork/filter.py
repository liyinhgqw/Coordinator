import coord.jobtool, coord.common
import os, sys
import random, time
from optparse import OptionParser

# I1 = '/home/stud/workspace/Coordinator/example/adwork/select ='
# O1 = '/home/stud/workspace/Coordinator/example/adwork/filter =' 

# <id, company name, bid, adBody>

SELECT_NUM = 100

class Ad(object):
  def __init__(self, id, name, bid, body):
    self.id = id
    self.name = name
    self.bid = int(bid)
    self.body = body
  def __str__(self):
    return self.id + '\t' + self.name + '\t' + str(self.bid) + '\t' + self.body
    
class Filter(coord.jobtool.JobTool):
  def run(self):
    lfs = coord.common.LFS()
    adlist = lfs.get_abs_subfiles(self.indirs['I1'].path[0])
    ads = []
    for adfile in adlist:
      for ad in open(adfile, 'r'):
        time.sleep(0.05)
        if random.randint(1, 100) < 90:
          id, name, bid, body = ad.split('\t')
          ads.append(Ad(id, name, bid, body))
      
    ads.sort(cmp=None, key=lambda ad: ad.bid, reverse=True)
    adlist = []
    for ad in ads:
      adlist.append(str(ad))
      
    # output
    outdir = self.outdirs['O1'].path[0]
    print '^^^', outdir
    lfs.mkdir(outdir)
    open(os.path.join(outdir, 'fad'), 'w').writelines(adlist)
    
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
  job = Filter(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob()