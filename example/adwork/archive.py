import coord.jobtool, coord.common
import os, sys
from optparse import OptionParser

# I1 = '/home/stud/workspace/Coordinator/example/adwork/filter =='
# O1 = '/home/stud/workspace/Coordinator/example/adwork/archive' 

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
    
class Archive(coord.jobtool.JobTool):
  def run(self):
    lfs = coord.common.LFS()
    adlist = []
    for indir in self.indirs['I1'].path:
      adlist.extend(lfs.get_abs_subfiles(indir))
    if lfs.exists(os.path.join(self.outputs['O1'].path, 'aad')):
      adlist.append(os.path.join(self.outputs['O1'].path, 'aad'))
    ads = []
    for adfile in adlist:
      for ad in open(adfile, 'r'):
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
    open(os.path.join(outdir, 'aad'), 'w').writelines(adlist)

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
  job = Archive(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob()