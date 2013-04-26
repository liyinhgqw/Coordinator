import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname):
    if self.get_output_subdirtotalnum(jobname) < 2:    # just to be conservative
      return True
    return False
  
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute('Archive')
#  proxy.execute_period_cond('Job1', proxy.need_input, 1.0)