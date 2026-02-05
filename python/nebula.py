import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from nebulagraph_python.client import NebulaClient

from config import Config

class Executor(object):
  def __init__(self, typ):
    self.typ = typ	
    self.cfg = Config()
  
  def run(self):
    total_latency = 0
    total_response = 0
    if self.typ == 'thread':
      with ThreadPoolExecutor(max_workers=self.cfg.concurrency) as executor:
        futures = []
        for _ in range(self.cfg.concurrency):
          futures.append(executor.submit(self.task))
        for future in futures:
          latency, response = future.result()
          total_latency += latency
          total_response += response
    elif self.typ == 'process':
      with ProcessPoolExecutor(max_workers=self.cfg.concurrency) as executor:
        futures = []
        for _ in range(self.cfg.concurrency):
          futures.append(executor.submit(self.task))
        for future in futures:
          latency, response = future.result()
          total_latency += latency
          total_response += response
    else:
      raise ValueError("Unknown executor type: {}".format(self.typ))
    return {
      "avg_latency_us": total_latency // (self.cfg.concurrency * self.cfg.iterations_per_concurrency),
      "avg_response_us": total_response // (self.cfg.concurrency * self.cfg.iterations_per_concurrency)}
    
    
  def task(self):
    total_latency = 0
    total_response = 0
    client = NebulaClient(self.cfg.address, self.cfg.user, self.cfg.password)
    for _ in range(self.cfg.iterations_per_concurrency):
      now = time.perf_counter_ns()
      result = client.execute(self.cfg.statement)
      total_latency += result.latency_us
      for _ in result:
         pass
      total_response += (time.perf_counter_ns() - now) // 1000
    return total_latency, total_response
