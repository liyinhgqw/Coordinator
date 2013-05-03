import coord.jobtool, coord.common
import os, sys
from optparse import OptionParser

# I1 = '/home/stud/workspace/Coordinator/example/adwork/ads =='
# O1 = '/home/stud/workspace/Coordinator/example/adwork/select =' (segs, rest)

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
    
class RankSelect(coord.jobtool.JobTool):
  def run(self):
    print 'RankSelect Running.'
    lfs = coord.common.LFS()
    adlist = []
    for indir in self.indirs['I1'].path:
      adlist.extend(lfs.get_abs_subfiles(indir))
    if lfs.exists(os.path.join(self.outputs['O1'].path, 'rest')):
      adlist.append(os.path.join(self.outputs['O1'].path, 'rest'))
    
    ads = []
    for adfile in adlist:
      for ad in open(adfile, 'r'):
        id, name, bid, body = ad.split('\t')
        ads.append(Ad(id, name, bid, body))
      
    ads.sort(cmp=None, key=lambda ad: ad.bid, reverse=True)
    adlist = []
    
    for ad in ads:
      adlist.append(str(ad))
    
    if len(adlist) <=0:
      exit(1)
      
    # output
    outdir = self.outdirs['O1'].path[0]
    outdir = self.mkdir('lfs', outdir)
    selectfile = open(os.path.join(outdir, 'sad'), 'w')
    selectfile.writelines(adlist[0:SELECT_NUM])
    restfile = open(os.path.join(self.outputs['O1'].path, 'rest'), 'w')
    restfile.writelines(adlist[SELECT_NUM:])

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
  job = RankSelect(options.jobname, options.indir, options.outdir, options.runtime)
  job.runjob(False)