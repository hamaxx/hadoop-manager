import os
import re
import shutil
import subprocess

from hdpmanager.hdpjob import HadoopJob
from hdpmanager.hdpfs import HadoopFs


HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')
TMP_FOLDER = '/tmp/hadoop-manager/'

class HadoopRunException(Exception):
	pass

class HadoopManager(object):

	HadoopRunException = HadoopRunException

	def __init__(self, hadoop_home, hadoop_fs_default_name=None, hadoop_job_tracker=None):
		self.rm_tmp_dir()

		self._hadoop_home = hadoop_home
		self._hadoop_fs_default_name = hadoop_fs_default_name
		self._hadoop_job_tracker = hadoop_job_tracker

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

	def _get_cmd_list(self, t):
		if not t:
			return []

		cmd = None
		if isinstance(t, (tuple, list)):
			if len(t) > 1 and t[-1] is None:
				# Skip command if attr is None
				return []
			cmd = list(t)
		else:
			cmd = [t]

		if len(cmd) == 2 and isinstance(cmd[1], list):
			exploded = []
			for attr in cmd[1]:
				exploded += [cmd[0], attr]
			cmd = exploded

		return [str(c) for c in cmd]

	def _run_hadoop_cmd(self, command, attrs):
		cmd = [self._hadoop_bin]

		cmd += self._get_cmd_list(command)

		if self._hadoop_fs_default_name:
			cmd += ['-D', 'fs.defaultFS=%s' % self._hadoop_fs_default_name,]
		if self._hadoop_job_tracker:
			cmd += ['-D', 'mapred.job.tracker=%s' % self._hadoop_job_tracker,]

		if not isinstance(attrs, list):
			attrs = [attrs]
		for attr in attrs:
			cmd += self._get_cmd_list(attr)

		hadoop = subprocess.Popen(cmd, stdout=subprocess.PIPE)
		self._print_lines(hadoop.stdout)

		hadoop.wait()
		if hadoop.returncode != 0:
			raise HadoopRunException('Hadoop streaming command failed!')

	def _print_lines(self, pipe):
		while True:
			o = pipe.readline()
			if not o:
				break
			print o,
		print

	def get_tmp_dir(self, subdir=None):
		path = TMP_FOLDER
		if subdir:
			path = os.path.join(path, subdir)
		if not os.path.exists(path):
			os.makedirs(path)
		return path

	def rm_tmp_dir(self):
		if os.path.exists(TMP_FOLDER):
			shutil.rmtree(TMP_FOLDER)

	def create_job(*args, **kwargs):
		return HadoopJob(*args, **kwargs)
