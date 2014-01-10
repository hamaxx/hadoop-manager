import sys
import traceback

from streamer import Streamer

class Mapper(Streamer):

	def _set_serializers(self, serializers):
		self._input_serializer = serializers['input']
		self._output_serializer = serializers['inter']

	def map(self, line):
		return line

	def parse_input(self):
		for line in self._input_stream:
			try:
				decoded_line = self._decode_component(line.rstrip('\t'), self._input_serializer)
				self._out(self.map(decoded_line))
			except Exception, e:
				self.count('error lines', 1)
				sys.stderr.write('error %s processing line:\n%s\n' % (repr(e), repr(line)))
				traceback.print_exc(file=sys.stderr)

