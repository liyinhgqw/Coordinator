import os, sys
import math
import numpy as np


if len(sys.argv) < 2:
  exit(1)
  
statdir = os.path.abspath(os.path.dirname(sys.argv[0]))
statfile = os.path.join(statdir, 'statfile')

if len(sys.argv) > 2:
  num = int(sys.argv[2])
else:
  num = -1
  cp
ads = []
bids = []
cnt = 0
for ad in open(sys.argv[1], 'r'):
  if num >= 0 and cnt >= num:
    break
  cnt = cnt + 1
  id, name, bid, body = ad.split('\t')
  bids.append(int(bid))
  ads.append(str(bid) + '\n')
  
statfd = open(statfile, 'w')
statfd.writelines(ads)

print np.mean(bids), np.std(bids)

