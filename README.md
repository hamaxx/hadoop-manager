hadoop-manager (alpha)
====

Python wrapper around Hadoop streaming jar.


###Install:

	pip install hadoop-manager


###Docs:

[Hadoop-manager docs](https://pythonhosted.org/hadoop-manager/)


###Basic Example

	from hdpmanager import HadoopManager
	from hdpmanager import Mapper
	from hdpmanager import Reducer


	class MyMapper(Mapper):
		def map(self, decoded):
			yield (decoded['site'], decoded['host']), 1
			self.count('map_ok', 1)

	class MyReducer(Reducer):
		def reduce(self, key, values):
			yield key, sum(values)
			self.count('reduce_ok', 1)


	if __name__ == "__main__":

		with HadoopManager(
				hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
				hadoop_fs_default_name='hdfs://hadoop/',
				hadoop_job_tracker='hdp01.example.com:8021',
			) as mng:

			job = mng.create_job(
					input_paths=['/user/ham/logs/test'],
					output_path='/user/ham/out',

					root_package='testapp',

					mapper='job_module.MyMapper',
					reducer='job_module.MyReducer',
					num_reducers=1,

					serialization=dict(input='json', output='json'),

					job_env=dict(requires=['simplejson']),
				)

			job.rm_output()

			job.run()

			for l in job.cat_output():
				print l

