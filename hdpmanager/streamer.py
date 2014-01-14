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


DEFAULT_INPUT_SERIALIZED = 'json'
DEFAULT_OUTPUT_SERIALIZED = 'json'
DEFAULT_INTER_SERIALIZED = 'pickle'


class _CacheSerializer(object):

	_cached_values = None

	def __init__(self, serializer, count):
		self._serializer = serializer
		self.count = count

		self._cached_values = {}

	def dumps(self, o, cache_idx=-1):
		if cache_idx < 0:
			return self._serializer.dumps(o)

		cached_value = self._cached_values.get(cache_idx)
		if cached_value and cached_value[0] == o:
			return cached_value[1]

		dumps = self._serializer.dumps(o)
		self._cached_values[cache_idx] = (o, dumps)

		return dumps

	def loads(self, s, cache_idx=-1):
		if cache_idx < 0:
			return self._serializer.loads(s)

		cached_value = self._cached_values.get(cache_idx)
		if cached_value and cached_value[1] == s:
			return cached_value[0]

		loads = self._serializer.loads(s)
		self._cached_values[cache_idx] = (loads, s)

		return loads


class PickleEscaped(object):

	def dumps(self, o):
		return pickle.dumps(o).encode('string_escape')

	def loads(self, s):
		return pickle.loads(s.decode('string_escape'))

class Json(object):

	def dumps(self, o):
		return json.dumps(o)

	def loads(self, s):
		return json.loads(s)

class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self.conf = self._get_env_conf(CONF_PICKE_FILE_PATH)

		counter = Counter(self.__class__.__name__)
		self.count = counter.count

		self._parse_serializers()

	def _get_env_conf(self, fn):
		if not os.path.exists(fn):
			return
		return pickle.load(open(fn))

	def _set_serializers(self, serializers):
		raise Exception('Must be implemented in Mapper/Reducer/Combiner')

	def _parse_serializers(self):
		serializer_objects = {
			'json' : _CacheSerializer(Json(), self.count),
			'pickle': _CacheSerializer(PickleEscaped(), self.count),
			'raw': None,
		}

		ser_conf = self._get_env_conf(SERIALIZATION_CONF_PICKE_FILE_PATH) or {}

		serializers = {
			'input': serializer_objects.get(ser_conf.get('input', DEFAULT_INPUT_SERIALIZED)),
			'output': serializer_objects.get(ser_conf.get('output', DEFAULT_OUTPUT_SERIALIZED)),
			'inter': serializer_objects.get(ser_conf.get('inter', DEFAULT_INTER_SERIALIZED)),
		}

		self._set_serializers(serializers)

	def _encode_component(self, comp, cache_idx=-1):
		if not self._encoder:
			return comp
		return self._encoder.dumps(comp, cache_idx)

	def _decode_component(self, comp, cache_idx=-1):
		if not self._decoder:
			return comp
		return self._decoder.loads(comp, cache_idx)

	def _out(self, outputs):
		if not outputs:
			return

		if isinstance(outputs, tuple):
			outputs = [outputs]

		for output in outputs:
			if not isinstance(output, tuple):
				raise Exception('Invalid output')

			output_serialized = [self._encode_component(x, cache_idx=i) for i, x in enumerate(output)]
			output_str = '\t'.join(output_serialized)

			self._output_stream.write(output_str + '\n')

	def _run(self):
		self.parse_input()
