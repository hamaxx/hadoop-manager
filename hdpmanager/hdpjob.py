import os
import re
import cPickle as pickle

import subprocess

from hdpenv import HadoopEnv

ZHDUTILS_PACKAGE = 'hdpmanager'
EGG_NAME = 'zemanta_hadoop_job'
EGG_VERSION = '1.0'

DEFAUT_NUM_REDUCERS = 10

HADOOP_STREAMING_JAR_RE = re.compile(r'^hadoop.*streaming.*\.jar$')

CONF_PICKE_FILE_PATH = 'streamer_conf.pickle'
SERIALIZATION_CONF_PICKE_FILE_PATH = 'serialization_conf.pickle'


class HadoopJob(object):

	def __init__(self, hdp_manager, input_paths, output_path, mapper, reducer=None, combiner=None, num_reducers=None, serialization=None, job_env=None, conf=None, root_package=None):

		self._hdpm = hdp_manager

		self._input_paths = input_paths
		self._output_path = output_path

		self._root_package = root_package

		self._mapper = mapper
		self._combiner = combiner
		self._reducer = reducer
		self._num_reducers = num_reducers or DEFAUT_NUM_REDUCERS

		self._serialization_conf = serialization
		self._serialization_conf_file = self._create_conf_file(serialization, SERIALIZATION_CONF_PICKE_FILE_PATH)

		self._conf = conf
		self._conf_file = self._create_conf_file(conf, CONF_PICKE_FILE_PATH)

		self._hadoop_env = HadoopEnv(hdp_manager, root_package=self._root_package, **(job_env or {}))

	def _create_conf_file(self, conf, fp):
		if not conf:
			return

		conf_file = os.path.join(self._hdpm.get_tmp_dir('conf'), fp)
		pickle.dump(conf, open(conf_file, 'w'))
		return conf_file

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

	def _get_combiner_command(self, encoded=True):
		return self._get_streamer_command(self._combiner, encoded)

	def run_local(self):
		import shutil

		env_files = self._hadoop_env.env_files

		env = {'PYTHONPATH': 'PYTHONPATH=:%s' % (os.pathsep.join([e[1] for e in env_files]))}

		if self._conf_file:
			shutil.copy2(self._conf_file, os.path.basename(self._conf_file))
		if self._serialization_conf_file:
			shutil.copy2(self._serialization_conf_file, os.path.basename(self._serialization_conf_file))

		mapper = subprocess.Popen(self._get_mapper_command(False), env=env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

		for path in self._input_paths:
			for line in open(path):
				mapper.stdin.write(line)
		mapper.stdin.close()

		out_stream = mapper.stdout.read()

		if self._combiner:
			combiner = subprocess.Popen(self._get_combiner_command(False), env=env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

			for line in sorted(out_stream.split('\n')):
				combiner.stdin.write(line + '\n')
			combiner.stdin.close()

			out_stream = combiner.stdout.read()

		if self._reducer:
			reducer = subprocess.Popen(self._get_reducer_command(False), env=env, stdout=subprocess.PIPE, stdin=subprocess.PIPE)

			for line in sorted(out_stream.split('\n')):
				reducer.stdin.write(line + '\n')
			reducer.stdin.close()

			out_stream = reducer.stdout.read()

		if self._conf_file:
			os.remove(os.path.basename(self._conf_file))
		if self._serialization_conf_file:
			os.remove(os.path.basename(self._serialization_conf_file))

		with open(self._output_path, 'w') as of:
			of.write(out_stream)

	def rm_output(self):
		cmd = [self._hdpm._hadoop_bin, 'fs',
			'-conf', self._hdpm._hadoop_config,
			'-rm', '-r', self._output_path,]

		hadoop = subprocess.Popen(cmd, stdout=subprocess.PIPE)
		self._hdpm._print_lines(hadoop.stdout)

	def run(self):
		env_files = self._hadoop_env.env_files

		cmd = [self._hdpm._hadoop_bin, 'jar', self._hdpm._hadoop_stream_jar,
			'-conf', self._hdpm._hadoop_config,
			'-mapper', self._get_mapper_command(),
			'-output', self._output_path,]

		for efile in env_files:
			cmd += ['-file', efile[1]]
		cmd += ['-cmdenv', 'PYTHONPATH=:%s' % (os.pathsep.join([e[0] for e in env_files]))]

		for path in self._input_paths:
			cmd += ['-input', path]

		if self._reducer:
			cmd += ['-reducer', self._get_reducer_command(),
				'-numReduceTasks', str(self._num_reducers)]

		if self._combiner:
			cmd += ['-combiner', self._get_combiner_command()]

		if self._conf_file:
			cmd += ['-file', self._conf_file]

		if self._serialization_conf_file:
			cmd += ['-file', self._serialization_conf_file]

		hadoop = subprocess.Popen(cmd, stdout=subprocess.PIPE, bufsize=1)
		self._hdpm._print_lines(hadoop.stdout)

		hadoop.wait()
		if hadoop.returncode != 0:
			raise Exception('Hadoop streaming command failed!')
