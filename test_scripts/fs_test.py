import os, sys
sys.path.append(os.path.pardir)

from hdpmanager import HadoopManager

with HadoopManager(
                hadoop_home='/opt/cloudera/parcels/CDH-4.2.0-1.cdh4.2.0.p0.10',
                hadoop_fs_default_name='hdfs://zedoop/',
                hadoop_job_tracker='hdp01.zemanta.com:8021') as mng:

    for f in mng.fs.ls('/user/ham/'):
        print f
