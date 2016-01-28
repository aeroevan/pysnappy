#!/usr/bin/env python
from setuptools import setup, Extension
have_cython = False
try:
    from Cython.Build import cythonize
    have_cython = True
except ImportError:
    pass


ext_modules=[]
if have_cython:
    ext_modules.append(
        Extension("pysnappy", sources=["pysnappy/pysnappy.pyx"],
                  libraries=["snappy"])
    )
else:
    ext_modules.append(
        Extension("pysnappy", sources=["pysnappy/pysnappy.c"],
                  libraries=["snappy"])
    )


setup(
    name='pysnappy',
    test_suite="tests",
    ext_modules=ext_modules,
)
