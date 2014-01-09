import simplejson as json

from hdpmanager import HadoopManager
from hdpmanager.mapper import Mapper
from hdpmanager.reducer import Reducer


class MyMapper(Mapper):

	def map(self, line):
		decoded = json.loads(line)
		self.count('map', 1)

		campaign = decoded['DATA']['campaign_id']
		cpc = decoded['DATA']['campaign_settings']['cpc_cc']

		return campaign, cpc

class MyReducer(Reducer):

	def reduce(self, key, values):
		self.count('reduce', 1)
		values = map(lambda x: float(x), values)
		return key, float(sum(values)) / 10000


if __name__ == "__main__":

	mng = HadoopManager(
			hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
			hadoop_config='hdfs_config.xml'
		)

	job = mng.create_job(
			input_paths=['/user/ham/clicks-2013-12-06_00016.tmp'],
			output_path='/user/ham/out.txt',

			mapper='subpackage.run_job.MyMapper',
			reducer='subpackage.run_job.MyReducer',
			combiner='subpackage.run_job.MyReducer',
			num_reducers=1,

			job_env=dict(requires=['simplejson'])
		)

	job.rm_output()
	job.run()

