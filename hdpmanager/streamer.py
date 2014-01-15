import os
import sys
import cPickle as pickle

from counter import Counter
from hdpjob import CONF_PICKE_FILE_PATH, SERIALIZATION_CONF_PICKE_FILE_PATH
from protocol import get_protocol_from_name


DEFAULT_INPUT_SERIALIZED = 'json'
DEFAULT_OUTPUT_SERIALIZED = 'json'
DEFAULT_INTER_SERIALIZED = 'pickle'


class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self._read_protocol = None
		self._write_protocol = None

		self.conf = self._get_env_conf(CONF_PICKE_FILE_PATH)

		counter = Counter(self.__class__.__name__)
		self.count = counter.count

		self._parse_serializers()

	def _get_env_conf(self, fn):
		if not os.path.exists(fn):
			return
		return pickle.load(open(fn))

	def _set_serializers(self, serializers):
		raise NotImplementedError('Must be implemented in Mapper/Reducer/Combiner')

	def _parse_serializers(self):
		ser_conf = self._get_env_conf(SERIALIZATION_CONF_PICKE_FILE_PATH) or {}

		serializers = {
			'input': get_protocol_from_name(ser_conf.get('input', DEFAULT_INPUT_SERIALIZED)),
			'output': get_protocol_from_name(ser_conf.get('output', DEFAULT_OUTPUT_SERIALIZED)),
			'inter': get_protocol_from_name(ser_conf.get('inter', DEFAULT_INTER_SERIALIZED)),
		}

		self._set_serializers(serializers)

	def _out(self, outputs):
		if not outputs:
			return

		if isinstance(outputs, tuple):
			outputs = [outputs]

		for output in outputs:
			if not isinstance(output, tuple):
				raise Exception('Invalid output')

			output_serialized = [self._write_protocol.encode(x, cache_idx=i) for i, x in enumerate(output)]
			output_str = '\t'.join(output_serialized)

			self._output_stream.write(output_str + '\n')

	def _run(self):
		self.parse_input()
