#!/usr/bin/env python
import os
import unittest
import pysnappy
from pysnappy.framing import HadoopDecompressor, HadoopCompressor


class RawTests(unittest.TestCase):

    def setUp(self):
        comp = os.path.join(os.path.dirname(__file__), 'iris.csv.raw')
        with open(comp, 'rb') as fh:
            self.compressed = fh.read()
        uncomp = os.path.join(os.path.dirname(__file__), 'iris.csv')
        with open(uncomp, 'rb') as fh:
            self.uncompressed = fh.read()

    def test_uncompress(self):
        uncompressed = pysnappy.uncompress(self.compressed)
        self.assertEqual(self.uncompressed, uncompressed,
                         'Uncompressed test failure')

    def test_compress(self):
        compressed = pysnappy.compress(self.uncompressed)
        self.assertEqual(self.compressed, compressed,
                         'Compressed test failure')

class HadoopTests(unittest.TestCase):

    def setUp(self):
        comp = os.path.join(os.path.dirname(__file__), 'iris.hadoop.snappy')
        with open(comp, 'rb') as fh:
            self.compressed = fh.read()
        uncomp = os.path.join(os.path.dirname(__file__), 'iris.csv')
        with open(uncomp, 'rb') as fh:
            self.uncompressed = fh.read()

    def test_uncompress(self):
        h = HadoopDecompressor()
        uncompressed = h.decompress(self.compressed)
        h.flush()
        self.assertEqual(self.uncompressed, uncompressed,
                         "Uncompressed test failure")

    def test_compress(self):
        h = HadoopCompressor()
        compressed = h.compress(self.uncompressed)
        h.flush()
        self.assertEqual(self.compressed, compressed,
                         "Compressed test failure")
