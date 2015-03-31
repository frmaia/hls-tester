#!/usr/bin/python

import m3u8
import time

from pprint import pprint
import os
import logging

TMP_BASE_DIR = "/tmp"

def __get_local_file_name(url):
	return "%s/%s" % ( TMP_BASE_DIR, url.split('/')[-1])

def download_http_resource(url):
	'''
	Use this method to download some resource through HTTP.
	'''

	import urllib2

	# TODO set timeout for download
	
	file_name = __get_local_file_name(url)
	logging.debug("DOWNLOADING '%s'" % (url))

	download_start_time = time.clock()

	u = urllib2.urlopen(url)

	f = open(file_name, 'wb')
	response_headers = u.info()
	file_size = int(response_headers.getheaders("Content-Length")[0])

	file_size_dl = 0
	block_sz = 8192
	while True:
		buffer = u.read(block_sz)
		if not buffer:
			break
	
		file_size_dl += len(buffer)
		f.write(buffer)

		### Print download percentage
		status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
		status = status + chr(8)*(len(status)+1)
		print status,
	
	f.close()

	download_end_time = time.clock()
	download_time = (download_end_time - download_start_time)

	#log_msg = "DOWNLOADED '%s' ; Bytes: '%s'" % (url, file_size)
	logging.debug("DOWNLOADED '%s' ; Bytes: '%s' in '%s' " % (url, file_size, download_time))

	return file_name, download_time, response_headers



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
		self.__probe()

		if self.response_headers:
			# Verify if request was cached by AWS CloudFront
			self.__check_cdn_cache(cache_header_param='x-cache', cache_header_value_contains='hit')

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
			tmp_file, self.download_time, self.response_headers = download_http_resource(self.url)
		except:
			logging.error("Error while trying to download chunk '%s'", self)
			return

		# Set size_in_bytes
		self.size_in_bytes = os.path.getsize(tmp_file)

		# Set bitrate
		self.bitrate = int(self.size_in_bytes * 8 / self.duration)

		#Delete file
		os.remove(tmp_file)



	def __probe(self, check_cdn_cache=False):
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

	def __check_cdn_cache(self,cache_header_param, cache_header_value_contains):
		
		if not self.response_headers:
			logging.debug("chunk response_headers is not defined")
			return False

		#logging.debug(self.response_headers)
		
		if not cache_header_param or not cache_header_value_contains:
			return False
		
		if cache_header_param in self.response_headers.dict and cache_header_value_contains in self.response_headers.dict[cache_header_param].lower():
			logging.info("CDN cache hit for '%s'" % self)
			return True

		logging.info("CDN cache miss for '%s'" % self)
		return False


def main(abr_stream_url):
	print abr_stream_url
	logging.info("STARTING...")
	try: 
		#TODO put data on a multiprocessing queue for running in a distributed way
		#TODO chunk duration time in this loop
		while True:
			logging.debug("Processing ABR playlist '%s'" % abr_stream_url)
			m3u8_obj = m3u8.load(abr_stream_url)
			for stream in m3u8_obj.playlists:
				pl_obj = m3u8.load(stream.absolute_uri)

				for segment in pl_obj.segments:
					logging.debug("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX") # ... it's just a helper for log viewers
					chunk = Chunk(segment.absolute_uri, segment.uri, segment.duration, stream.stream_info.bandwidth)
					#pprint(vars(chunk))

				#print pl_obj
				#pprint(vars(m3u8_obj))
	except KeyboardInterrupt:
		logging.info("FINISHED by a 'KeyboardInterrupt' !")

	

if __name__ == "__main__":

	logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
	
	#TODO: define external configurations
	abr_stream_url = "http://url-for-your-origin-or-edge/abr-playlist.m3u8"
	main(abr_stream_url)

	


