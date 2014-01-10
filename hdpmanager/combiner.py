from reducer import Reducer

class Combiner(Reducer):

	def _set_serializers(self, serializers):
		self._input_serializer = serializers['inter']
		self._output_serializer = serializers['inter']
