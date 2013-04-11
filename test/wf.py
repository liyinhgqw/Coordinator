'''
Workflow Example
'''

import coord.client

class Proxy(coord.client):
  def need_input(self, jobname, injob):
    if self.get_buffered_output_subdirnum(jobname) < 2:    # just to be conservative
      return True
    return False
  
if __name__ == '__main__':
  master = coord.common.localhost() + ':' + str(coord.common.MASTER_PORT)
  proxy = Proxy(master)
  proxy.execute_period_cond('Job1', proxy.need_input, 1.0)
  