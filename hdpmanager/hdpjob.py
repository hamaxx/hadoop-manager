import os
import re

import subprocess

from hdpenv import HadoopEnv

EGG_NAME = 'zemanta_hadoop_job'
EGG_VERSION = '1.0'

ZHDUTILS_PACKAGE = 'hdpmanager'

DEFAUT_NUM_REDUCERS = 10

HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')


class HadoopJob(object):

	def __init__(self, hdp_manager, input_paths, output_path, mapper, reducer=None, combiner=None, num_reducers=None, job_env=None):

		self._hdpm = hdp_manager

		self._input_paths = input_paths
		self._output_path = output_path

		self._mapper = mapper
		self._combiner = combiner
		self._reducer = reducer
		self._num_reducers = num_reducers or DEFAUT_NUM_REDUCERS

		self._hadoop_env = HadoopEnv(module_paths=[self._mapper, self._reducer, self._combiner], **(job_env or {}))

	def _get_streamer_command(self, module_path, encoded):
		path = module_path.split('.')
		module = '.'.join(path[:-1])
		class_name = path[-1]

		if encoded:
			return 'python -c "from %s import %s; %s()._run()"' % (module, class_name, class_name)
		else:
			return 'python', '-c', 'from %s import %s; %s()._run()' % (module, class_name, class_name)

	def _get_mapper_command(self, encoded=True):
		return self._get_streamer_command(self._mapper, encoded)

	def _get_reducer_command(self, encoded=True):
		return self._get_streamer_command(self._reducer, encoded)

	def run_local(self):
		env = {'PYTHONPATH': os.path.abspath(self._hadoop_env.env_files[1])}

		mapper = subprocess.Popen(self._get_mapper_command(False), env=env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

		for path in self._input_paths:
			for line in open(path):
				mapper.stdin.write(line)
		mapper.stdin.close()

		out_stream = mapper.stdout.read()

		if self._reducer:
			reducer = subprocess.Popen(self._get_reducer_command(False), env=env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

			for line in sorted(out_stream.split('\n')):
				reducer.stdin.write(line + '\n')
			reducer.stdin.close()

			out_stream = reducer.stdout.read()

		with self._output_path as of:
			out_stream.write(of)

	def rm_output(self):
		cmd = [self._hdpm._hadoop_bin, 'fs',
			'-conf', self._hdpm._hadoop_config,
			'-rm', '-r', self._output_path,]

		hadoop = subprocess.Popen(cmd, stdout=subprocess.PIPE)
		self._hdpm._print_lines(hadoop.stdout)

	def run(self):
		egg = self._hadoop_env.env_files

		cmd = [self._hdpm._hadoop_bin, 'jar', self._hdpm._hadoop_stream_jar,
			'-conf', self._hdpm._hadoop_config,
			'-mapper', self._get_mapper_command(),
			'-file', egg[1],
			'-cmdenv', 'PYTHONPATH=%s' % egg[0],
			'-output', self._output_path,
		]

		for path in self._input_paths:
			cmd += ['-input', path]

		cmd += []

		if self._reducer:
			cmd += ['-reducer', self._get_reducer_command(),
				'-numReduceTasks', str(self._num_reducers),]

		if self._combiner:
			cmd += ['-combiner', self._get_reducer_command()]

		hadoop = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
		self._hdpm._print_lines(hadoop.stdout)

