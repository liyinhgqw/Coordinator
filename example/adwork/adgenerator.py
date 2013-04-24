'''
Created on Apr 23, 2013

@author: stud
'''

import sys
import os
import random
import coord.jobtool


class AdGenerator(coord.jobtool.JobTool):
  def run(self):
    print "run"
    self.outfile = open(os.path.join(self.outdir, "ads"), 'w')
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
  global interval, batchsize
  interval = float(sys.argv[1])
  batchsize = int(sys.argv[2])
  
  adgenerator = AdGenerator("AdGenerator", "adgenerator_in", "adgenerator_out")
  adgenerator.runjob()