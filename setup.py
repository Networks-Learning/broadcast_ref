from distutils.core import setup

import numpy
from Cython.Build import cythonize
from Cython.Distutils import Extension

extensions = [Extension("*", ["*.pyx"])]

setup(
    ext_modules=cythonize(extensions),
    include_dirs=[numpy.get_include()]
)
