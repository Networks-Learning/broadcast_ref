from distutils.core import setup

import numpy
from Cython.Build import cythonize
from setuptools import Extension

extensions = [Extension("opt", ["opt/*.pyx"]), Extension("data", ["data/*.pyx"])]

setup(
    ext_modules=cythonize(extensions),
    include_dirs=[numpy.get_include()]
)
