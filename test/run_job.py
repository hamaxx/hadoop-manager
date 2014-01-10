import sys

from hdpmanager import HadoopManager
from hdpmanager.mapper import Mapper
from hdpmanager.reducer import Reducer
from hdpmanager.combiner import Combiner


class MyMapper(Mapper):

	def map(self, decoded):
		self.count('map_lalala', 1)

		campaign = decoded['DATA']['campaign_id']
		blog = decoded['DATA']['blog_id']
		cpc = decoded['DATA']['campaign_settings']['cpc_cc']

		yield (blog, campaign), cpc

class MyReducer(Reducer):

	def reduce(self, key, values):
		self.count('reduce_lalala', 1)

		yield key, float(sum(values))

class MyCombiner(MyReducer, Combiner):
	pass

if __name__ == "__main__":

	mng = HadoopManager(
			hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
			hadoop_config='test/hdfs_config.xml'
		)

	job = mng.create_job(
			input_paths=['/user/ham/clicks-2013-12-06_00016.tmp'],
			output_path='/user/ham/out.txt',

			mapper='test.run_job.MyMapper',
			reducer='test.run_job.MyReducer',
			combiner='test.run_job.MyCombiner',
			num_reducers=1,

			serialization=dict(input='json', output='json', inter='pickle'),

			job_env=dict(requires=['simplejson'])
		)

	job.rm_output()
	job.run()

	#job._input_paths = ['test/test_in.json']
	#job._output_path = 'out.txt'
	#job.run_local()

