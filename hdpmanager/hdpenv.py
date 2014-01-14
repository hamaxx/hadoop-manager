import os
import sys


EGG_NAME = 'hadoop_job'
EGG_VERSION = '1.0'

ZHDUTILS_PACKAGE = 'hdpmanager'


class HadoopEnv(object):

	def __init__(self, packages=None, package_data=None, requires=None, module_paths=None, root_package=None):

		self._root_package = root_package

		self._packages = packages or []
		if not packages and module_paths:
			self._packages += self._get_packages_from_module_paths(module_paths)

		self._package_data = package_data

		self._requires = requires or []
		self._requires += [ZHDUTILS_PACKAGE]

		self.env_files = self._package()

	def _setup_working_dir(self):
		import importlib

		if not self._root_package:
			return

		sys.path.insert(0, os.getcwd())
		mod = importlib.import_module(self._root_package)
		path = os.path.abspath(mod.__path__[0])

		os.chdir(path)

	def _get_packages_from_module_paths(self, module_paths):
		packages = set()
		for path in module_paths:
			if not path: continue
			parts = path.split('.')
			packages = packages.union(['.'.join(parts[:i+1]) for i in xrange(len(parts[:-2]))])
		return list(packages)

	def _package(self):
		cwd = os.getcwd()
		self._setup_working_dir()

		packaged_egg = self._build_egg()
		packaged_requires = self._package_requires()

		os.chdir(cwd)

		return packaged_egg + packaged_requires

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

		return [(egg_filename, os.path.abspath('dist/' + egg_filename))]

	def _get_module_package(self, module):
		import importlib
		import shutil

		mod = importlib.import_module(module)

		try:
			path = mod.__path__[0]
		except AttributeError:
			path = mod.__file__

		if os.path.isdir(path):	# Package in a folder
			shutil.make_archive('dist/%s' % mod.__package__, 'zip', os.path.normpath(os.path.join(path, '..')), mod.__package__)
			fname = 'dist/%s.zip' % mod.__package__
			dname = 'lib/%s.zip' % mod.__package__
			return dname, os.path.abspath(fname)

		elif os.path.splitext(os.path.normpath(os.path.join(path, '..')))[1] == '.egg': # Package in an egg
			egg_path = os.path.normpath(os.path.join(path, '..'))
			return os.path.basename(egg_path), egg_path

		elif os.path.splitext(path)[1] == '.so': # Binary module
			return os.path.basename(path), os.path.abspath(path)

		raise Exception('Unsupported package type')

	def _package_requires(self):
		# Prototype. Should work sometimes.

		packages = []
		for module in self._requires:
			if isinstance(module, (unicode, str)):
				packages.append(self._get_module_package(module))
			else:
				raise Exception('Invalid requirement parameter')

		return packages
