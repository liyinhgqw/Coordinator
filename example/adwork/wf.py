import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname):
    if self.get_output_subdirtotalnum(jobname) < 2:    # just to be conservative
      return True
    return False
  
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute_period('RankSelect', interval = 0.1)
  proxy.execute_period('Filter', interval = 0.1)
  proxy.execute_period('Archive', interval = 0.1)
  
#  proxy.execute_period_cond('Job1', proxy.need_input, 1.0)