import coord.client
import time

class Coord(coord.client.Client):
  pass
  
def test(val, msg):
  if val:
    print '**************************', msg
    
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.begin_checkblock("wf")
  test(proxy.execute('RankSelect'), 'RankSelect')
  test(proxy.execute_dep('Identity1', 'RankSelect'), 'Identity1')
  time.sleep(5)
  test(proxy.execute_dep('Filter', 'Identity1'), 'Filter')
  test(proxy.execute_dep('Archive', 'Filter'), 'Archive')
  proxy.end_checkblock("wf")
