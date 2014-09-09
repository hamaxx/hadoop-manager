from hdpmanager import HadoopManager
from hdpmanager import Mapper
from hdpmanager import Reducer
from hdpmanager import Combiner

class MyMapper(Mapper):

    #def line_grep(self):
    #       return re.compile('google.com')

    def map(self, decoded):
        import ujson
        yield 'aa', 1
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

    mng = HadoopManager(
                    hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
                    hadoop_fs_default_name='hdfs://zedoop/',
                    hadoop_job_tracker='hdp01.zemanta.com:8021',
            )

    job = mng.create_job(
                    input_paths=['/user/badger/logs/pageviews/pageviews-2014-01-10_00001'],
                    output_path='/user/ham/out.txt',

                    root_package='test_scripts',
                    mapper='subpackage.run_job.MyMapper',
                    reducer='subpackage.run_job.MyReducer',
                    combiner='subpackage.run_job.MyCombiner',
                    num_reducers=1,

                    serialization=dict(input='json', output='pickle', inter='pickle'),

                    job_env=dict(requires=['ujson']),
            )

    #job.rm_output()
    #job.run()
    #print '\n'.join(str(l) for l in job.cat_output())

    job._input_paths = ['test_scripts/test_in.json']
    job._output_path = 'out.txt'
    job.run_local()
