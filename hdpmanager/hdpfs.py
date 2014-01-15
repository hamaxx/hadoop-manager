from protocol import get_protocol_from_name

class HadoopFs(object):

	def __init__(self, hadoop_manager):
		self._hdpm = hadoop_manager

	def cat(self, path, serializer='raw', tab_seperated=False):
		output = self._hdpm._run_hadoop_cmd('fs', ('-cat', path))
		output_serializer = get_protocol_from_name(serializer)

		for line in output:
			line = line.rstrip()

			if tab_seperated:
				yield tuple(output_serializer.decode(part) for part in line.split('\t'))
			else:
				yield output_serializer.decode(line)

	def rm(self, path):
		self._hdpm._run_hadoop_cmd_echo('fs', ('-rm', '-r', path))
