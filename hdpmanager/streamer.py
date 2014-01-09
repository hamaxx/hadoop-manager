import os
import sys
import pickle
import json

from counter import Counter
from hdpjob import CONF_PICKE_FILE_PATH

class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self.conf = self._get_env_conf()

		counter = Counter(self.__class__.__name__)
		self.count = counter.count

	def _get_env_conf(self):
		if not os.path.exists(CONF_PICKE_FILE_PATH):
			return
		return pickle.load(open(CONF_PICKE_FILE_PATH))

	def _encode_component(self, comp):
		if comp is None:
			return str(None)
		if isinstance(comp, str):
			return comp
		if isinstance(comp, unicode):
			return comp.encode('ascii', errors='replace')
		if isinstance(comp, (int, long,  float)):
			return str(comp)

		return json.dumps(comp)

	def _out(self, outputs):
		if not outputs:
			return

		if isinstance(outputs, tuple):
			outputs = [outputs]

		for output in outputs:
			if not isinstance(output, tuple):
				raise Exception('Invalid output')

			output_str = '\t'.join(map(lambda x: self._encode_component(x), output))
			self._output_stream.write(output_str + '\n')

	def _run(self):
		self.parse_input()
