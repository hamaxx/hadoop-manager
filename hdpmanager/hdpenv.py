import os
import sys


EGG_NAME = 'hadoop_job'
EGG_VERSION = '1.0'

ZHDUTILS_PACKAGE = 'hdpmanager'


class HadoopEnv(object):

	def __init__(self, packages=None, package_data=None, requires=None, module_paths=None):

		self._packages = [ZHDUTILS_PACKAGE]
		if packages:
			self._packages += packages
		if module_paths:
			self._packages += self._get_packages_from_module_paths(module_paths)

		self._package_data = package_data
		self._requires = requires

		self.env_files = self._package()

	def _get_packages_from_module_paths(self, module_paths):
		# TODO: Fix this quick and dirty prototype

		packages = set()
		for path in module_paths:
			if not path: continue
			packages.add(path.split('.')[0])
		return list(packages)

	def _package(self):
		packaged_egg = self._build_egg()
		packaged_requires = self._package_requires()

		return packaged_requires + packaged_egg

	def _build_egg(self):
		import setuptools

		attrs={
			'name': EGG_NAME,
			'version': '1.0',

			'script_name': __file__,
			'zip_safe': False,
		}

		if self._packages:
			attrs['packages'] = self._packages
		if self._package_data:
			attrs['package_data'] = self._package_data
		#if self._requires:
		#	attrs['install_requires'] = self._requires

		dist = setuptools.Distribution(attrs)
		dist.run_command('bdist_egg')

		egg_python_version = '%s.%s' % (sys.version_info.major, sys.version_info.minor)
		egg_filename = '%s-%s-py%s.egg' % (EGG_NAME, EGG_VERSION, egg_python_version)

		return [(egg_filename, 'dist/' + egg_filename)]

	def _package_requires(self):
		import importlib
		import shutil

		packages = []
		for module in self._requires:
			mod = importlib.import_module(module)
			if not mod.__package__:
				packages.append((os.path.basename(mod.__file__), mod.__file__))
			else:
				shutil.make_archive('dist/%s' % mod.__package__, 'zip', os.path.normpath(os.path.join(mod.__path__[0], '..')), mod.__package__)
				fname = 'dist/%s.zip' % mod.__package__
				dname = 'lib/%s.zip' % mod.__package__
				packages.append((dname, fname))
		return packages
