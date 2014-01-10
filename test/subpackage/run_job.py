from hdpmanager import HadoopManager
from hdpmanager.mapper import Mapper
from hdpmanager.reducer import Reducer
from hdpmanager.combiner import Combiner


class MyMapper(Mapper):

	def map(self, line):
		test = self.conf['test']
		return test, 1

class MyReducer(Reducer):

	def reduce(self, key, values):
		self.count('reduce', 1)
		values = map(lambda x: float(x), values)
		return key, float(sum(values)) / 10000

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

			root_package='test',

			mapper='subpackage.run_job.MyMapper',
			reducer='subpackage.run_job.MyReducer',
			combiner='subpackage.run_job.MyCombiner',
			num_reducers=1,

			conf=dict(test=12345),

			job_env=dict(requires=['simplejson'])
		)

	job.rm_output()
	job.run()

	#job._input_paths = ['test/test_in.json']
	#job._output_path = 'out.txt'
	#job.run_local()
