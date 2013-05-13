import coord.client

class Coord(coord.client.Client):
  def need_input(self, jobname, *args, **kw):
    if len(kw) > 0:
      depjob = kw['dep']
      if self.get_buffered_input_subdirtotalnum(depjob) < 3:
        return True
      else:
        return False
    return True
      
if __name__ == '__main__':
  proxy = Coord('10.0.2.15')
  proxy.execute_period_cond('RankSelect', proxy.need_input, 0.1, dep = 'Identity1')
  proxy.execute_period_cond('Identity1', proxy.need_input, 0.1, dep = 'Filter')
  proxy.execute_period_cond('Filter', proxy.need_input, 0.1, dep = 'Archive')
  proxy.execute_period('Archive', interval = 0.1)
  
