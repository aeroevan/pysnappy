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
        Extension("pysnappy.core", sources=["pysnappy/core.pyx"],
                  libraries=["snappy"])
    )
    ext_modules.append(
        Extension("pysnappy.framing", sources=["pysnappy/framing.pyx"],
                  libraries=["snappy"])
    )
    ext_modules.append(
        Extension("pysnappy.crc32c", sources=["pysnappy/crc32c.pyx"])
    )
else:
    ext_modules.append(
        Extension("pysnappy", sources=["pysnappy/core.c"],
                  libraries=["snappy"])
    )
    ext_modules.append(
        Extension("pysnappy.framing", sources=["pysnappy/framing.c"],
                  libraries=["snappy"])
    )
    ext_modules.append(
        Extension("pysnappy.crc32c", sources=["pysnappy/crc32c.c"])
    )


setup(
    name='pysnappy',
    test_suite="tests",
    ext_modules=ext_modules,
)
