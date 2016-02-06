#!/usr/bin/env python
from setuptools import setup, Extension
have_cython = False
try:
    import Cython
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
    name="pysnappy",
    version="0.5.0",
    description="Cython bindings to libsnappy",
    url="https://github.com/aeroevan/pysnappy",
    license='MIT',
    test_suite="tests",
    ext_modules=ext_modules,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Cython"
    ]
)
