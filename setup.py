#!/usr/bin/env python
from setuptools import setup, Extension
from Cython.Build import cythonize

ext_modules=[
    Extension("pysnappy",
              sources=["pysnappy.pyx"],
              libraries=["snappy"]
    )
]


setup(
    name='pysnappy',
    ext_modules=ext_modules,
)
