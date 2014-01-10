import os
import sys
import cPickle as pickle
import binascii

try:
	try:
		import ujson as json
	except ImportError:
		import simplejson as json
except ImportError:
	import json

from counter import Counter
from hdpjob import CONF_PICKE_FILE_PATH, SERIALIZATION_CONF_PICKE_FILE_PATH


class pickleBase64(object):
	def dumps(self, o):
		return binascii.b2a_base64(pickle.dumps(o, -1)).rstrip() # faster than base64 module
	def loads(self, s):
		return pickle.loads(binascii.a2b_base64(s))

serializer_objects = {
	'json' : json,
	'pickle': pickleBase64(),
	'raw': None,
}

class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self.conf = self._get_env_conf(CONF_PICKE_FILE_PATH)

		self._parse_serializers()

		counter = Counter(self.__class__.__name__)
		self.count = counter.count

	def _get_env_conf(self, fn):
		if not os.path.exists(fn):
			return
		return pickle.load(open(fn))

	def _set_serializers(self, serializers):
		raise Exception('Must be implemented in Mapper/Reducer/Combiner')

	def _parse_serializers(self):
		ser_conf = self._get_env_conf(SERIALIZATION_CONF_PICKE_FILE_PATH) or {}
		serializers = {
			'input': serializer_objects.get(ser_conf.get('input', 'json')),
			'output': serializer_objects.get(ser_conf.get('output', 'json')),
			'inter': serializer_objects.get(ser_conf.get('inter', 'pickle')),
		}
		self._set_serializers(serializers)

	def _encode_component(self, comp, serializer):
		if not serializer:
			return comp
		return serializer.dumps(comp)

	def _decode_component(self, comp, serializer):
		if not serializer:
			return comp
		return serializer.loads(comp)

	def _out(self, outputs):
		if not outputs:
			return

		if isinstance(outputs, tuple):
			outputs = [outputs]

		for output in outputs:
			if not isinstance(output, tuple):
				raise Exception('Invalid output')

			output_str = '\t'.join(map(lambda x: self._encode_component(x, self._output_serializer), output))
			self._output_stream.write(output_str + '\n')

	def _run(self):
		self.parse_input()
