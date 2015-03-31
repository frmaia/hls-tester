import logging
import time
import urllib2


class httphelper:
	
	@staticmethod
	def download_http_resource(url, file_name):
		'''
		Use this method to download some resource through HTTP.
		'''
		# TODO set timeout for download
		logging.debug("DOWNLOADING '%s'" % (url))

		download_start_time = time.time()

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
			#status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
			#status = status + chr(8)*(len(status)+1)
			#print status,
		
		f.close()

		download_end_time = time.time()
		download_time = (download_end_time - download_start_time)

		#log_msg = "DOWNLOADED '%s' ; Bytes: '%s'" % (url, file_size)
		logging.debug("DOWNLOADED '%s' ; Bytes: '%s' in '%s' " % (url, file_size, download_time))

		return download_time, response_headers
