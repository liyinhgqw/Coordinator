
import sys, os
import coord.jobtool
import leveldb

class RankSelect(coord.jobtool.JobTool):
  def run(self):
    self.file = open(os.path.join(self.indir, 'ads'), 'r')
    for ad in self.file:
      print ad
      
if __name__ == '__main__':
  rankselect = RankSelect("RankSelect", 'rankselect_in', None)
  rankselect.runjob()