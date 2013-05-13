import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname, *args, **kw):
    buffer = self.get_buffered_input_subdirtotalnum('Filter') + self.get_buffered_input_subdirtotalnum('Identity1')
    start_phase = self.get_stat('RankSelect', 'runtime') < 0 or self.get_stat('Identity1', 'runtime') < 0 or self.get_stat('Filter', 'runtime') < 0
    if start_phase:
      ratio = 2
    else:
      ratio = (self.get_stat('RankSelect', 'runtime') + self.get_stat('Identity1', 'runtime')) / self.get_stat('Filter', 'runtime')
      
#    print '************************', ratio
    if buffer <= ratio:
      return True
    return False
      
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute_period_cond('RankSelect', proxy.need_input, 0.1)
  proxy.execute_period('Identity1', 0.1)
  proxy.execute_period('Filter', 0.1)
  proxy.execute_period('Archive', 0.1)
  
