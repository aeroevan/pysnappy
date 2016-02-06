#!/usr/bin/env python
import os
import unittest
import pysnappy
from pysnappy.framing import HadoopDecompressor, HadoopCompressor
from pysnappy.framing import RawDecompressor, RawCompressor
from pysnappy.framing import Decompressor, Compressor


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

    def test_framed_uncompress(self):
        r = RawDecompressor()
        uncompressed = r.decompress(self.compressed)
        r.flush()
        self.assertEqual(self.uncompressed, uncompressed,
                         'Uncompressed test failure')

    def test_framed_compress(self):
        r = RawCompressor()
        compressed = r.compress(self.uncompressed)
        r.flush()
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


class Framing2Tests(unittest.TestCase):
    def setUp(self):
        comp = os.path.join(os.path.dirname(__file__), 'iris.framing2.sz')
        self.comp_fh = open(comp, 'rb')
        uncomp = os.path.join(os.path.dirname(__file__), 'iris.csv')
        self.uncomp_fh = open(uncomp, 'rb')

    def tearDown(self):
        self.comp_fh.close()
        self.uncomp_fh.close()

    def test_uncompress(self):
        decompressor = Decompressor()
        self.comp_fh.seek(0)
        self.uncomp_fh.seek(0)
        while True:
            buf = self.comp_fh.read(65536)
            if not buf:
                break
            buf = decompressor.decompress(buf)
            if buf:
                buf2 = self.uncomp_fh.read(len(buf))
                self.assertEqual(buf, buf2, "Uncompress failure")
        decompressor.flush()

    def test_compress(self):
        compressor = Compressor()
        self.comp_fh.seek(0)
        self.uncomp_fh.seek(0)
        while True:
            buf = self.uncomp_fh.read(65536)
            if not buf:
                break
            buf = compressor.compress(buf)
            if buf:
                buf2 = self.comp_fh.read(len(buf))
                self.assertEqual(buf, buf2, "Compress failure")
        compressor.flush()
