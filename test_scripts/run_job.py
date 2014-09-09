import os, sys
sys.path.append(os.path.join(os.path.dirname(__file__), os.path.pardir))

from hdpmanager import HadoopManager
from hdpmanager import Mapper
from hdpmanager import Reducer


class MyMapper(Mapper):

    def map(self, decoded):
        yield 123
        self.count('map_ok', 1)

class MyReducer(Reducer):
    def reduce(self, key, values):
        yield key, len(values)
        self.count('reduce_ok', 1)

if __name__ == "__main__":

    with HadoopManager(
            hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
            hadoop_fs_default_name='hdfs://zedoop/',
            hadoop_job_tracker='hdp01.zemanta.com:8021') as mng:

        job = mng.create_job(
                input_paths=['/user/badger/logs/pageviews/pageviews-2014-01-10_00001'],
                output_path='/user/ham/out.txt',

                root_package='test_scripts',

                mapper='run_job.MyMapper',
                reducer='run_job.MyReducer',
                num_reducers=1,

                serialization=dict(input='json', output='pickle', inter='pickle'),

                job_env=dict(requires=[('ujson', 'ujson.so')]),
        )

        job.rm_output()
        job.run()
        print '\n'.join(str(l) for l in job.cat_output())

    #job._input_paths = ['test_scripts/test_in.json']
    #job._output_path = 'out.txt'
    #job.run_local()
