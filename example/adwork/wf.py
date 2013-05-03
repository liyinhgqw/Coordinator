import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname, *args, **kw):
    if self.get_output_subdirtotalnum(jobname) < 2:    # just to be conservative
      return True
    return False
  
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute_period('RankSelect', interval = 0.1)
  proxy.execute_period('Identity1', interval = 0.1)
  proxy.execute_period('Identity2', interval = 0.1)
  proxy.execute_period('Identity3', interval = 0.1)
  proxy.execute_period('Filter', interval = 0.1)
  proxy.execute_period('Archive', interval = 0.1)
  
