'''
Created on Apr 23, 2013

@author: stud
'''

import random
import sys, os
import coord.jobtool, coord.common
from genericpath import getsize

HOME = '/home/stud/workspace/Coordinator/example/adwork'
ADS_PATH = os.path.join(HOME, 'ads')
REST_PATH = os.path.join(HOME, 'select', 'rest')

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

def check_success():
  lfs = coord.common.LFS()
  # Decide run or not according to the buffer size
  totalsz = lfs.get_unfinished_dir_size(ADS_PATH, 'RankSelect')
  if lfs.exists(REST_PATH):
    totalsz += getsize(REST_PATH)
  if totalsz > 1500000:
    print 'block'
    return False
  return True

if __name__ == '__main__':
  global batchsize
  batchsize = int(sys.argv[1])
  
  CHECK = True
  
  if not CHECK or check_success():
    adgenerator = AdGenerator('10.0.2.15', "AdGenerator", None, None)
    adgenerator.runjob()