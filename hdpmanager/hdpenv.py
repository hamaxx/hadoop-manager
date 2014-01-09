import os
import sys


EGG_NAME = 'hadoop_job'
EGG_VERSION = '1.0'

ZHDUTILS_PACKAGE = 'hdpmanager'


class HadoopEnv(object):

	def __init__(self, packages=None, package_data=None, requires=None, module_paths=None):

		self._packages = packages or []
		if not packages and module_paths:
			self._packages += self._get_packages_from_module_paths(module_paths)

		self._package_data = package_data

		self._requires = requires or []
		self._requires += [ZHDUTILS_PACKAGE]

		self.env_files = self._package()

	def _get_packages_from_module_paths(self, module_paths):
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

		dist = setuptools.Distribution(attrs)
		dist.run_command('bdist_egg')

		egg_python_version = '%s.%s' % (sys.version_info.major, sys.version_info.minor)
		egg_filename = '%s-%s-py%s.egg' % (EGG_NAME, EGG_VERSION, egg_python_version)

		return [(egg_filename, 'dist/' + egg_filename)]

	def _package_requires(self):
		# Prototype. Should work sometimes.

		import importlib
		import shutil

		packages = []
		for module in self._requires:
			mod = importlib.import_module(module)

			try:
				path = mod.__path__[0]
			except AttributeError:
				path = mod.__file__

			if os.path.isdir(path):	# Package in a folder
				shutil.make_archive('dist/%s' % mod.__package__, 'zip', os.path.normpath(os.path.join(path, '..')), mod.__package__)
				fname = 'dist/%s.zip' % mod.__package__
				dname = 'lib/%s.zip' % mod.__package__
				packages.append((dname, fname))

			elif os.path.splitext(os.path.normpath(os.path.join(path, '..')))[1] == '.egg': # Package in an egg
				egg_path = os.path.normpath(os.path.join(path, '..'))
				packages.append((os.path.basename(egg_path), egg_path))

			elif os.path.splitext(path)[1] == '.so': # Binary module
				packages.append((os.path.basename(path), path))

		return packages
