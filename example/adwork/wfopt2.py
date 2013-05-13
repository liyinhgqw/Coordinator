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

  
  proxy.execute_period_cond('Identity1', proxy.need_input, 0.1, dep = 'Identity2')
  proxy.execute_period_cond('Identity2', proxy.need_input, 0.1, dep = 'Identity3')
  proxy.execute_period_cond('Identity3', proxy.need_input, 0.1, dep = 'Identity4')
  proxy.execute_period_cond('Identity4', proxy.need_input, 0.1, dep = 'Identity5')
  proxy.execute_period_cond('Identity5', proxy.need_input, 0.1, dep = 'Identity6')
  proxy.execute_period_cond('Identity6', proxy.need_input, 0.1, dep = 'Identity7')
  proxy.execute_period_cond('Identity7', proxy.need_input, 0.1, dep = 'Identity8')
  proxy.execute_period_cond('Identity8', proxy.need_input, 0.1, dep = 'Identity9')
  proxy.execute_period_cond('Identity9', proxy.need_input, 0.1, dep = 'Identity10')
  
  proxy.execute_period_cond('Identity10', proxy.need_input, 0.1, dep = 'Filter')
  
  proxy.execute_period_cond('Filter', proxy.need_input, 0.1, dep = 'Archive')
  proxy.execute_period('Archive', interval = 0.1)
  
