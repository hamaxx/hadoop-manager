import os
import sys
import pickle

from counter import count
from hdpjob import CONF_PICKE_FILE_PATH 

class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self.conf = self._get_env_conf()

		self.count = count

	def _get_env_conf(self):
		if not os.path.exists(CONF_PICKE_FILE_PATH):
			return
		return pickle.load(open(CONF_PICKE_FILE_PATH))

	def _out(self, outputs):
		if not outputs:
			return

		if not isinstance(outputs, (list, tuple)):
			outputs = [outputs]

		output = '\t'.join(map(lambda x: str(x), outputs))
		self._output_stream.write(output + '\n')

	def _run(self):
		self.parse_input()
