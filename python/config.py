import os

class Config(object):
		def __init__(self):
			self.address = os.getenv("NEBULA_ADDRESS")
			self.user = os.getenv("NEBULA_USER")
			self.password = os.getenv("NEBULA_PASSWORD")
			self.statement = os.getenv("NEBULA_STATEMENT")
			self.concurrency = int(os.getenv("NEBULA_CONCURRENCY", "1"))
			self.iterations_per_concurrency = int(os.getenv("NEBULA_ITERATIONS_PER_CONCURRENCY", "10"))
   
