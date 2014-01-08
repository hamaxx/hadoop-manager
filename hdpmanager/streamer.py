import sys

from counter import count


class Streamer(object):

	def __init__(self, input_stream=sys.stdin, output_stream=sys.stdout):
		self._input_stream = input_stream
		self._output_stream = output_stream

		self.count = count

	def _out(self, outputs):
		if not outputs:
			return

		if not isinstance(outputs, (list, tuple)):
			outputs = [outputs]

		output = '\t'.join(map(lambda x: str(x), outputs))
		self._output_stream.write(output + '\n')

	def _run(self):
		self.parse_input()
