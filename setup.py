from distutils.core import setup

import numpy
from Cython.Build import cythonize
from setuptools import Extension

extensions = [Extension("opt", ["broadcast/opt/*.pyx"]),
              Extension("data", ["broadcast/data/*.pyx"])]

setup(
    name='broadcast_ref',
    version='0.0.1',
    packages=['broadcast'],
    package_dir={
        'broadcast': 'broadcast'
    },
    ext_modules=cythonize(extensions),
    include_dirs=[numpy.get_include()]
)
