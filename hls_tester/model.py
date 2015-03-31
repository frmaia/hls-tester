
from hls_tester.helper import httphelper

import logging
import os
import tempfile

class Chunk:
	def __init__(self, absolute_url, uri, duration, stream_bandwidth):
		self.url = absolute_url
		self.uri = uri
		self.duration = duration
		self.stream_bandwidth = stream_bandwidth

		self.bitrate = None
		self.size_in_bytes = None
		self.download_time = None
		self.response_headers = None

		self.__get_details()

		#self.probe()

		#if self.response_headers:
		#	# Verify if request was cached by AWS CloudFront
		#	self.check_cdn_cache(cache_header_param='x-cache', cache_header_value_contains='hit')

	def __str__(self):
		return str(vars(self))

	def is_download_time_ok(self):
		'''
		Check if chunk download time is less than chunk duration, 
		which must avoid player buffering.
		'''
		return float(self.download_time) < float(self.duration)

	def is_bitrate_ok(self):
		'''
		Check if bitrate is ok for the bandwidth.
		'''
		return self.size_in_bytes < self.stream_bandwidth


	def __get_details(self):
		'''
			This method downloads the chunk and 'feeds' the following informations:
			- download_time
			- bitrate (the real bitrate based on file size and duration)
			- size_in_bytes (chunk file size in bytes)
		'''
		try:
			# Download chunk to get more detailed information
			tmp_file = tempfile.NamedTemporaryFile().name
			self.download_time, self.response_headers = httphelper.download_http_resource(self.url, tmp_file)
		except:
			logging.error("Error while trying to download chunk '%s'", self)
			return

		# Set size_in_bytes
		self.size_in_bytes = os.path.getsize(tmp_file)

		# Set bitrate
		self.bitrate = int(self.size_in_bytes * 8 / self.duration)

		#Delete file
		os.remove(tmp_file)


	def probe(self, check_cdn_cache=False):
		if not self.is_download_time_ok():
			logging.warn("Download time exceeded chunk duration. It must cause a buffering behavior for '%s'" % self)
			return False
		else:
			logging.debug("Download time is OK for chunk '%s'" % self)

		if not self.is_bitrate_ok():
			logging.warn("Chunk bitrate (%s) is greater than the stream bandwidth (%s) for '%s'." % (self.bitrate, self.stream_bandwidth, self.url) )
			return False
		else:
			logging.debug("Bitrate is OK for chunk '%s'" % self)

		return True

	def check_cdn_cache(self, cache_header_param, cache_header_value_contains):
		
		if not self.response_headers:
			logging.debug("chunk response_headers is not defined")
			return False

		#logging.debug(self.response_headers)
		
		if not cache_header_param or not cache_header_value_contains:
			return False
		
		if cache_header_param in self.response_headers.dict and cache_header_value_contains in self.response_headers.dict[cache_header_param].lower():
			logging.info("CDN_CACHE_HIT for '%s'" % self.url)
			logging.debug("CDN_CACHE_HIT for '%s'" % self)
			return True

		logging.info("CDN_CACHE_MISS for '%s'" % self.url)
		logging.debug("CDN_CACHE_MISS for '%s'" % self)
		return False
