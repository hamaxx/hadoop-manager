import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

import json

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
		values = map(lambda x: int(x), values)
		return key, float(sum(values)) / 10000


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
			num_reducers=1,

			setup=dict(packages=['test'])
		)

	job.rm_output()
	job.run()

