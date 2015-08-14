from multiprocessing import Pool, Process, Queue

class AnalyticsManager(Process):
	def __init__(self, analytics_queue, process_name):
		super(AnalyticsManager, self).__init__()
		self.name  = process_name
		self.analytics_queue = analytics_queue

		self.total_chunks = 0
		self.total_cache_hit = 0
		self.total_cache_miss = 0
		self.total_problematic_downloads = 0
		self.total_problematic_bitrates = 0

	def run(self):
		logging.info("AnalyticsManager '%s' is running" % self.name)
		while True:
			
			for data in iter( self.analytics_queue.get, None ):
				logging.info("'%s' consuming '%s'" % (self.name, data))
				self.sum(data)
				self.print_metrics()

			time.sleep(3)

	def sum(self, data):
		self.total_chunks += 1
		
		if 'cache_hit' in data:
			if data['cache_hit']:
				self.total_cache_hit += 1
			else:
				self.total_cache_miss += 1
				
		if 'total_problematic_downloads' in data and not data['download_ok']:
			self.total_problematic_downloads += 1

		if 'total_problematic_bitrates' in data and not data['bitrate_ok']:
			self.total_problematic_bitrates += 1

	def print_metrics(self):
		logging.info(vars(self))