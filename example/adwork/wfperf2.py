import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname, *args, **kw):
    buffer = self.get_buffered_input_subdirtotalnum('Filter') + self.get_buffered_input_subdirtotalnum('Identity1') \
      + self.get_buffered_input_subdirtotalnum('Identity2') \
      + self.get_buffered_input_subdirtotalnum('Identity3') \
      + self.get_buffered_input_subdirtotalnum('Identity4') \
      + self.get_buffered_input_subdirtotalnum('Identity5') \
      + self.get_buffered_input_subdirtotalnum('Identity6') \
      + self.get_buffered_input_subdirtotalnum('Identity7') \
      + self.get_buffered_input_subdirtotalnum('Identity8') \
      + self.get_buffered_input_subdirtotalnum('Identity9') \
      + self.get_buffered_input_subdirtotalnum('Identity10')
    
    start_phase = self.get_stat('RankSelect', 'runtime') < 0 \
      or self.get_stat('Identity1', 'runtime') < 0 \
      or self.get_stat('Identity2', 'runtime') < 0 \
      or self.get_stat('Identity3', 'runtime') < 0 \
      or self.get_stat('Identity4', 'runtime') < 0 \
      or self.get_stat('Identity5', 'runtime') < 0 \
      or self.get_stat('Identity6', 'runtime') < 0 \
      or self.get_stat('Identity7', 'runtime') < 0 \
      or self.get_stat('Identity8', 'runtime') < 0 \
      or self.get_stat('Identity9', 'runtime') < 0 \
      or self.get_stat('Identity10', 'runtime') < 0 \
      or self.get_stat('Filter', 'runtime') < 0
      
    if start_phase:
      ratio = 12
    else:
      ratio = (self.get_stat('RankSelect', 'runtime') + self.get_stat('Identity1', 'runtime') \
               + self.get_stat('Identity2', 'runtime') \
               + self.get_stat('Identity3', 'runtime') \
               + self.get_stat('Identity4', 'runtime') \
               + self.get_stat('Identity5', 'runtime') \
               + self.get_stat('Identity6', 'runtime') \
               + self.get_stat('Identity7', 'runtime') \
               + self.get_stat('Identity8', 'runtime') \
               + self.get_stat('Identity9', 'runtime') \
               + self.get_stat('Identity10', 'runtime')) / self.get_stat('Filter', 'runtime')
      
#    print '************************', ratio
    if buffer <= ratio:
      return True
    return False
      
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute_period_cond('RankSelect', proxy.need_input, 0.1)
  proxy.execute_period('Identity1', 0.1)
  proxy.execute_period('Identity2', 0.1)
  proxy.execute_period('Identity3', 0.1)
  proxy.execute_period('Identity4', 0.1)
  proxy.execute_period('Identity5', 0.1)
  proxy.execute_period('Identity6', 0.1)
  proxy.execute_period('Identity7', 0.1)
  proxy.execute_period('Identity8', 0.1)
  proxy.execute_period('Identity9', 0.1)
  proxy.execute_period('Identity10', 0.1)
  
  proxy.execute_period('Filter', 0.1)
  proxy.execute_period('Archive', 0.1)
  
