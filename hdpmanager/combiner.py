from reducer import Reducer

class Combiner(Reducer):

	def _set_serializers(self, serializers):
		self._read_protocol = serializers['inter']
		self._write_protocol = serializers['inter']
