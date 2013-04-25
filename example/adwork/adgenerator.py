'''
Created on Apr 23, 2013

@author: stud
'''

import random
import sys, os
import coord.jobtool, coord.common

HOME = '/home/stud/workspace/Coordinator/example/adwork'

class AdGenerator(coord.jobtool.JobTool):
  def run(self):
    lfs = coord.common.LFS()
    segpath = self.next_seg('lfs', os.path.join(HOME, 'ads'))
    lfs.mkdir(segpath)
    self.outfile = open(os.path.join(segpath, 'ad'), 'w')
    self.generate()

  def gen_id(self):
    return random.randint(1, 1000000000)
  
  def gen_bid(self):
    return random.randint(1, 100000)

  def generate_record(self):
    id = self.gen_id()
    name = 'AD:' + str(id)
    bid = self.gen_bid()
    adbody = (name + '.com', name + '.content')
    
    record = str(id) + '\t' + name + '\t' + str(bid) + '\t' + (str(adbody))[1:-1] + '\n'
    return record
  
  def generate(self):
    global batchsize
    for i in range(batchsize):
      self.outfile.write(self.generate_record())  

if __name__ == '__main__':
  global batchsize
  batchsize = int(sys.argv[1])
  
  adgenerator = AdGenerator("AdGenerator", None, None)
  adgenerator.runjob()