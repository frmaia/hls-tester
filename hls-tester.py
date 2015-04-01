#!/usr/bin/python

from hls_tester.model import Chunk
from hls_tester.helper import httphelper
from multiprocessing import Pool, Process, Queue
from pprint import pprint

import json
import logging
import m3u8
import os
import random
import sys
import tempfile
import time


def check_cdn_cache(chunk, cache_header_param, cache_header_value_contains):
	
	if not chunk.response_headers:
		logging.debug("chunk response_headers is not defined")
		return False

	#logging.debug(chunk.response_headers)
	
	if not cache_header_param or not cache_header_value_contains:
		return False
	
	if cache_header_param in chunk.response_headers.dict and cache_header_value_contains in chunk.response_headers.dict[cache_header_param].lower():
		logging.info("CDN_CACHE_HIT for '%s'" % chunk.url)
		logging.debug("CDN_CACHE_HIT for '%s'" % chunk)
		return True

	logging.info("CDN_CACHE_MISS for '%s'" % chunk.url)
	logging.debug("CDN_CACHE_MISS for '%s'" % chunk)
	return False


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


		

class Worker(Process):
	def __init__(self, job_queue, worker_name, analytics_queue):
		super(Worker, self).__init__()
		self.job_queue = job_queue
		self.name  = worker_name
		self.analytics_queue = analytics_queue

	def run(self):
		logging.info("Worker '%s' is running" % self.name)
		for data in iter( self.job_queue.get, None ):
			logging.info("'%s' consuming '%s'" % (self.name, data))
			self.__process_m3u8(data, self)

	def __process_m3u8(self, abr_stream_url, worker):
		logging.info("STARTING...")

		#TODO chunk duration time in this loop
		logging.debug("Processing ABR playlist '%s'" % abr_stream_url)
		m3u8_obj = m3u8.load(abr_stream_url)
		for stream in m3u8_obj.playlists:
			pl_obj = m3u8.load(stream.absolute_uri)

			for segment in pl_obj.segments:
				#logging.debug("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX - %s" % worker.name) # ... it's just a helper for log viewers
				logging.info("%s processing '%s'" % (worker.name,segment.absolute_uri) )# ... it's just a helper for log viewers
				chunk = Chunk(segment.absolute_uri, segment.uri, segment.duration, stream.stream_info.bandwidth)
				chunk.probe()

				# Verify if request was cached by AWS CloudFront
				cache_hitted = check_cdn_cache(chunk, cache_header_param='x-cache', cache_header_value_contains='hit')	
				self.__send_data_to_manager(cache_hitted, chunk.is_download_time_ok(), chunk.is_bitrate_ok())

	def __send_data_to_manager(self, bool_cache_hit, bool_download_ok, bool_bitrate_ok):
		analytics_data = {'cache_hit': bool_cache_hit, 'download_ok': bool_download_ok, 'bitrate_ok': bool_bitrate_ok}
		self.analytics_queue.put(analytics_data)

	

if __name__ == "__main__":

	if len(sys.argv) < 3:
		print "Use: time python2.7 %s <number_of_workers> <abr_stream_url_1> [abr_stream_url_2] [abr_stream_url_3] [...] " % sys.argv[0]
		sys.exit("Example: time python %s 32 'http://url-for-your-origin-or-edge/abr-playlist-1.m3u8' 'http://url-for-your-origin-or-edge/abr-playlist-2.m3u8'" % sys.argv[0])

	logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

	number_of_workers = int(sys.argv[1])
	abr_stream_url = sys.argv[2]
	
	# Prepare the ABR (Adaptive Bit Rate) HLS manifests that will be probed
	urls = []
	for i in sys.argv[2:]:
		urls.append(i)
	
	# Configure queues
	job_queue = Queue()
	analytics_queue = Queue()

	# Configure worker for Analytics Manager 
	AnalyticsManager( analytics_queue , "analytics-manager" ).start()

	# Ensures that every worker will have something to do
	number_of_stream_viewers  = number_of_workers
	if len(urls) > number_of_workers:
		number_of_stream_viewers = len(urls)

	# Instantiates workers that will do the 'hard job'
	for i in range(number_of_workers):
		worker_name = "worker_%s" % i
		Worker( job_queue , worker_name, analytics_queue ).start()

	# Loop until user's interruption
	try:
		while True:
			for i in range(1,number_of_stream_viewers):
				logging.debug("Putting '%s' data items on queue" % number_of_stream_viewers)
				job_queue.put( random.choice(urls) )
			time.sleep(10)
	except KeyboardInterrupt:
		logging.info("FINISHED by a 'KeyboardInterrupt' !")


	# Sentinel objects to allow clean shutdown: 1 per worker.
	for i in range(number_of_workers):
		job_queue.put( None )

	analytics_queue.put( None )
		


