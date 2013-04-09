'''
Created on Apr 9, 2013

@author: stud
'''
import time
import coord.common
import os

if __name__ == '__main__':
  jobname = 'Job1'
  finished_tag = os.path.join(coord.common.SLAVE_META_PATH, jobname + '_' + 
                                         coord.common.FINISHED_TAG)
  
  print 'test begins.'
  time.sleep(20)
  print('test ends.')
  
  os.mknod(finished_tag)
  