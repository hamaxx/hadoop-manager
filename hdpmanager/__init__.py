import os
import re

from hdpmanager.hdpjob import HadoopJob
from hdpmanager.hdpfs import HadoopFs

HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')


class HadoopManager(object):

	def __init__(self, hadoop_home, hadoop_config=None):

		self._hadoop_home = hadoop_home
		self._hadoop_config = hadoop_config

		self._hadoop_bin = self._find_hadoop_bin()
		self._hadoop_stream_jar = self._find_streaming_jar()

		self.fs = HadoopFs(self)

	def _find_hadoop_bin(self):
		return '%s/bin/hadoop' % self._hadoop_home

	def _find_streaming_jar(self):
		paths = [os.path.join(self._hadoop_home, 'lib', 'hadoop-0.20-mapreduce', 'contrib'), # Try 4.0 path first
			self._hadoop_home]

		for path in paths:
			for (dirpath, _, filenames) in os.walk(path):
				for filename in filenames:
					if HADOOP_STREAMING_JAR_RE.match(filename):
						return os.path.join(dirpath, filename)
		return None

	def _print_lines(self, pipe):
		while True:
			o = pipe.readline()
			if not o:
				break
			print o,
		print

	def create_job(*args, **kwargs):
		return HadoopJob(*args, **kwargs)
