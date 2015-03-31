#!/usr/bin/python

from hls_tester.model import Chunk
from hls_tester.helper import httphelper
from multiprocessing import Pool, Process, Queue
from pprint import pprint

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

		

class Worker(Process):
	def __init__(self, queue, worker_name):
		super(Worker, self).__init__()
		self.queue = queue
		self.name  = worker_name

	def run(self):
		logging.info("Worker '%s' is running" % self.name)
		for data in iter( self.queue.get, None ):
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

			if chunk.response_headers:
				# Verify if request was cached by AWS CloudFront
				check_cdn_cache(chunk, cache_header_param='x-cache', cache_header_value_contains='hit')				
	

if __name__ == "__main__":

	if len(sys.argv) < 3:
		print "Use: time python2.7 %s <number_of_workers> <abr_stream_url_1> [abr_stream_url_2] [abr_stream_url_3] [...] " % sys.argv[0]
		sys.exit("Example: time python %s 32 'http://url-for-your-origin-or-edge/abr-playlist-1.m3u8' 'http://url-for-your-origin-or-edge/abr-playlist-2.m3u8'" % sys.argv[0])

	logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)

	number_of_workers = int(sys.argv[1])
	abr_stream_url = sys.argv[2]
	
	urls = []
	for i in sys.argv[2:]:
		urls.append(i)
	
	request_queue = Queue()


	number_of_stream_viewers  = number_of_workers
	if len(urls) > number_of_workers:
		number_of_stream_viewers = len(urls)


	for i in range(number_of_workers):
		worker_name = "worker_%s" % i
		Worker( request_queue , worker_name ).start()

	try:
		while True:
			for i in range(1,number_of_stream_viewers):
				logging.debug("Putting '%s' data items on queue" % number_of_stream_viewers)
				request_queue.put( random.choice(urls) )
			time.sleep(10)
	except KeyboardInterrupt:
		logging.info("FINISHED by a 'KeyboardInterrupt' !")


	# Sentinel objects to allow clean shutdown: 1 per worker.
	for i in range(number_of_workers):
		request_queue.put( None )
		


