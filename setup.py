#!/usr/bin/env python
from setuptools import setup, find_packages, Extension
have_cython = False
try:
    import Cython
    have_cython = True
except ImportError:
    pass


ext_modules = []
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
        Extension("pysnappy.core", sources=["pysnappy/core.c"],
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
    version="0.6.1",
    description="Cython bindings to libsnappy",
    author="Evan McClain",
    author_email="aeroevan@gmail.com",
    url="https://github.com/aeroevan/pysnappy",
    download_url="https://github.com/aeroevan/pysnappy/tarball/v0.6.1",
    license='MIT',
    test_suite="tests",
    packages=find_packages(exclude="test"),
    ext_modules=ext_modules,
    entry_points={
        'console_scripts': [
            'pysnappy = pysnappy.cli:main'
        ]
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Cython"
    ]
)
