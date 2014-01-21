import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

import urlparse

from hdpmanager import HadoopManager
from hdpmanager import Mapper
from hdpmanager import Reducer
from hdpmanager import Combiner


class MyMapper(Mapper):

	def map(self, decoded):
		url = decoded['REQUEST'].get('prior_referrer', '')
		blog_id = decoded['REQUEST'].get('blog_id', '')
		if not url: return
		host = urlparse.urlparse(url).hostname.strip()
		yield (blog_id, host), 1
		self.count('map_ok', 1)

class MyReducer(Reducer):
	def reduce(self, key, values):
		yield key, sum(values)
		self.count('reduce_ok', 1)

class MyCombiner(Combiner):
	def reduce(self, key, values):
		yield key, sum(values)
		self.count('combiner_ok', 1)

if __name__ == "__main__":

	with HadoopManager(
		hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
		hadoop_fs_default_name='hdfs://zedoop/',
		hadoop_job_tracker='hdp01.zemanta.com:8021') as mng:

		job = mng.create_job(
			input_paths=['/user/badger/logs/pageviews/pageviews-2014-01-10_00001'],
			output_path='/user/ham/out.txt',

			mapper='test_scripts.run_job.MyMapper',
			reducer='test_scripts.run_job.MyReducer',
			combiner='test_scripts.run_job.MyCombiner',
			num_reducers=1,

			serialization=dict(input='json', output='pickle', inter='pickle'),

			job_env=dict(requires=[('ujson', 'test_scripts/ujson.so')]),
		)

		job.rm_output()
		job.run()
		print '\n'.join(str(l) for l in job.cat_output())

	#job._input_paths = ['test_scripts/test_in.json']
	#job._output_path = 'out.txt'
	#job.run_local()


