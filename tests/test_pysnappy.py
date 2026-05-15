from pathlib import Path

import pytest

import pysnappy
from pysnappy.framing import (
    Compressor,
    Decompressor,
    HadoopCompressor,
    HadoopDecompressor,
    RawCompressor,
    RawDecompressor,
)

DATA_DIR = Path(__file__).parent


@pytest.fixture
def uncompressed():
    return (DATA_DIR / "iris.csv").read_bytes()


class TestRaw:
    def test_roundtrip(self, uncompressed):
        assert pysnappy.uncompress(pysnappy.compress(uncompressed)) == uncompressed

    def test_framed_roundtrip(self, uncompressed):
        c = RawCompressor()
        compressed = c.compress(uncompressed)
        c.flush()
        d = RawDecompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == uncompressed


class TestHadoop:
    def test_roundtrip(self, uncompressed):
        c = HadoopCompressor()
        compressed = c.compress(uncompressed)
        c.flush()
        d = HadoopDecompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == uncompressed


class TestFraming2:
    def test_roundtrip(self, uncompressed):
        c = Compressor()
        compressed = c.compress(uncompressed)
        c.flush()
        d = Decompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == uncompressed
