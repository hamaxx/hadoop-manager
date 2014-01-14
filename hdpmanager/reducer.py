import sys
import traceback

from streamer import Streamer


class Reducer(Streamer):

	def _set_serializers(self, serializers):
		self._decoder = serializers['inter']
		self._encoder = serializers['output']

	def reduce(self, key, values):
		return key, values

	def _try_reduce(self, key, values):
		try:
			self._out(self.reduce(key, values))
		except Exception, e:
			self.count('error records', 1)
			sys.stderr.write('error %s processing record; key:%s, values:\n' % (repr(e), repr(key)))
			traceback.print_exc(file=sys.stderr)

	def parse_line(self, line):
		parts = line.split('\t')
		key = self._decode_component(parts[0], cache_idx=0)
		values = map(lambda x: self._decode_component(x, cache_idx=1), parts[1:])

		return key, values

	def parse_input(self):
		last_key = None
		all_values = []

		for line in self._input_stream:
			line = line.rstrip()
			if not line:
				continue

			key, values = self.parse_line(line)

			if last_key != key and last_key != None:
				self._try_reduce(last_key, all_values)
				all_values = []

			last_key = key
			all_values += values

		if last_key is not None:
			self._try_reduce(last_key, all_values)
