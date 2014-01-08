import sys
import traceback

from streamer import Streamer


class Reducer(Streamer):

	def reduce(self, key, values):
		return key, values

	def _try_reduce(self, key, values):
		try:
			self._out(self.reduce(key, values))
		except Exception, e:
			self.count('error records', 1)
			sys.stderr.write('error %s processing record; key:%s, values:\n' % (repr(e), repr(key)))
			for value in values:
				sys.stderr.write(repr(value))
				sys.stderr.write('\n')
			traceback.print_exc(file=sys.stderr)

	def parse_input(self):
		last_key = None
		values = []
		for line in self._input_stream:
			line = line.rstrip()
			if not line:
				continue

			parts = line.split('\t')
			key = parts[0]
			value = parts[1]

			if last_key != key and last_key != None:
				self._try_reduce(last_key, values)
				values = []

			last_key = key
			values.append(value)

		if last_key is not None:
			self._try_reduce(last_key, values)
