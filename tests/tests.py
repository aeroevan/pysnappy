#!/usr/bin/env python
import os
import unittest
import pysnappy


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