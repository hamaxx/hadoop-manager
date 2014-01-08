import sys

EGG_NAME = 'zemanta_hadoop_job'
EGG_VERSION = '1.0'

ZHDUTILS_PACKAGE = 'hdpmanager'

class HadoopEnv(object):

	def __init__(self, packages=None, package_data=None, requires=None):

		self._packages = [ZHDUTILS_PACKAGE]
		if packages:
			self._packages += packages

		self._package_data = package_data
		self._requires = requires

		self.env_files = self._build_egg()

	def _build_egg(self):
		import setuptools

		attrs={
			'name': 'zemanta_hadoop_job',
			'version': '1.0',

			'script_name': __file__,
			'zip_safe': False,
		}

		if self._packages:
			attrs['packages'] = self._packages
		if self._package_data:
			attrs['package_data'] = self._package_data
		if self._requires:
			attrs['install_requires'] = self._requires

		dist = setuptools.Distribution(attrs)
		dist.run_command('bdist_egg')

		egg_python_version = '%s.%s' % (sys.version_info.major, sys.version_info.minor)
		egg_filename = '%s-%s-py%s.egg' % (EGG_NAME, EGG_VERSION, egg_python_version)

		return (egg_filename, 'dist/' + egg_filename)


