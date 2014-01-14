import sys
import traceback

from streamer import Streamer

class Mapper(Streamer):

	def _set_serializers(self, serializers):
		self._decoder = serializers['input']
		self._encoder = serializers['inter']

	def map(self, line):
		return line

	def parse_line(self, line):
		return self._decode_component(line.rstrip('\t'))

	def parse_input(self):
		for line in self._input_stream:
			try:
				self._out(self.map(self.parse_line(line)))
			except Exception, e:
				self.count('error lines', 1)
				sys.stderr.write('error %s processing line:\n%s\n' % (repr(e), repr(line)))
				traceback.print_exc(file=sys.stderr)

