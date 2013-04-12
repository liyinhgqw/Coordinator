'''
Workflow Example
'''

import coord.client

class Proxy(coord.client.Client):
  def need_input(self, jobname):
    if self.get_output_subdirtotalnum(jobname) < 2:    # just to be conservative
      return True
    return False
  
if __name__ == '__main__':
  master = coord.common.localhost() + ':' + str(coord.common.MASTER_PORT)
  proxy = Proxy(master)
  proxy.execute_cond('Job1', proxy.need_input, False)
#  proxy.execute_period_cond('Job1', proxy.need_input, 1.0)
  