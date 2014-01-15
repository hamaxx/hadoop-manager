import sys
import urlparse

import ujson as json

from hdpmanager import HadoopManager
from hdpmanager.mapper import Mapper
from hdpmanager.reducer import Reducer
from hdpmanager.combiner import Combiner


class MyMapper(Mapper):

	def map(self, line):

		decoded = json.loads(line)

		url = decoded['REQUEST'].get('prior_referrer', '')
		blog_id = decoded['REQUEST'].get('blog_id', '')

		if not url:
			return

		host = urlparse.urlparse(url).hostname.strip()

		yield (blog_id, host), 1

		self.count('map_ok_fino', 1)

class MyReducer(Reducer):

	def reduce(self, key, values):

		yield repr(key), sum(values)

		self.count('reduce_ok', 1)

class MyCombiner(Combiner):
	def reduce(self, key, values):
		uniq = set(values)
		yield key, list(uniq)

if __name__ == "__main__":

	mng = HadoopManager(
			hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
			hadoop_config='test/hdfs_config.xml'
		)

	job = mng.create_job(
			input_paths=['/user/badger/logs/pageviews/pageviews-2014-01-10_000*'],
			output_path='/user/ham/out.txt',

			mapper='test.run_job.MyMapper',
			reducer='test.run_job.MyReducer',
			#combiner='test.run_job.MyCombiner',
			num_reducers=1,

			serialization=dict(input='raw', output='json', inter='pickle'),

			job_env=dict(requires=['ujson'])
		)

	#job.rm_output()
	#job.run()

	job._input_paths = ['test/test_in.json']
	job._output_path = 'out.txt'
	job.run_local()

