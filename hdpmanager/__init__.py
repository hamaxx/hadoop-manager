"""
	hadoop-manager
	~~~~~

	Python wrapper around Hadoop streaming jar.
"""

__author__ = 'Jure Ham <jure.ham@zemanta.com>'

__all__ = ['HadoopManager', 'Mapper', 'Reducer', 'Combiner']


from hdpmanager import HadoopManager

from mapper import Mapper
from reducer import Reducer
from combiner import Combiner
