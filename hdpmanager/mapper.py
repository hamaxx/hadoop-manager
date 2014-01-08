import sys
import traceback

from streamer import Streamer

class Mapper(Streamer):

	def map(self, line):
		return line

	def parse_input(self):
		for line in self._input_stream:
			try:
				self._out(self.map(line.rstrip()))
			except Exception, e:
				self.count('error lines', 1)
				sys.stderr.write('error %s processing line:\n%s\n' % (repr(e), repr(line)))
				traceback.print_exc(file=sys.stderr)

