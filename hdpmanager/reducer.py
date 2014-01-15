__all__ = ['Reducer']

import sys
import traceback

from streamer import Streamer


class Reducer(Streamer):

	def _set_serializers(self, serializers):
		self._read_protocol = serializers['inter']
		self._write_protocol = serializers['output']

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
		line = line.rstrip()
		parts = line.split('\t')

		if not parts or len(parts) < 2:
			raise AttributeError('Empty line')

		key = self._read_protocol.decode(parts[0], cache_idx=0)
		values = [self._read_protocol.decode(x, cache_idx=1) for x in parts[1:]]

		return key, values

	def parse_input(self):
		last_key = None
		all_values = []

		for line in self._input_stream:
			try:
				key, values = self.parse_line(line)
			except AttributeError:
				continue

			if last_key != key and last_key != None:
				self._try_reduce(last_key, all_values)
				all_values = []

			last_key = key
			all_values += values

		if last_key is not None:
			self._try_reduce(last_key, all_values)
