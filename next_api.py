from hdpmanager import HadoopManager
from hdpmanager import protocols
from hdpmanager import MapRed

from . import OtherJob


HadoopManager.global_config(
    hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
    hadoop_fs_default_name='hdfs://zedoop/',
    hadoop_job_tracker='hdp01.zemanta.com:8021',
)


class SimpleJob(MapRed):

    def mapper(self, line):
        yield line, 1

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


class RealJob(MapRed):

    INPUT_PROTOCOL = protocols.JsonProtocol
    OUTPUT_PROTOCOL = protocols.JsonProtocol

    def map_input(self, line):
        yield line, 1

    def reduce_input(self, key, values):
        yield key, sum(values)

    def find_max(self, key, values):
        yield key, max(values)

    def steps(self):
        return [
            self.step(
                mapper=self.map_input,
                reducer=self.reduce_input,
            ).
            self.step(
                reducer=self.find_max,
            ),
        ]


if __name__ == '__main__':
    simple_job = SimpleJob('/path/to/file').run()
    real_job = RealJob([SimpleJob('/path/to/file'), OtherJob('/path/to/file2')]).run()
