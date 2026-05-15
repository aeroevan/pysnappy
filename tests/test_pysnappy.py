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

    def test_roundtrip_multi_buffer(self, uncompressed):
        # Default HadoopCompressor buffer is 131072 bytes; force several
        # buffer-spanning chunks to exercise the multi-buffer branch.
        data = uncompressed * 256
        assert len(data) > 131072
        c = HadoopCompressor()
        compressed = c.compress(data)
        c.flush()
        d = HadoopDecompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == data

    def test_roundtrip_single_subblock(self, uncompressed):
        data = uncompressed * 256
        c = HadoopCompressor(single_subblock=True)
        compressed = c.compress(data)
        c.flush()
        # Cross-decode: single-subblock output must be readable by the same
        # decompressor that handles the multi-subblock variant.
        d = HadoopDecompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == data

    def test_single_subblock_emits_one_subblock(self, uncompressed):
        # Sanity-check the on-wire shape: 4-byte block size + 4-byte subblock
        # size + compressed payload, with no further subblocks.
        import struct
        data = uncompressed * 256
        compressed = HadoopCompressor(single_subblock=True).compress(data)
        block_size = struct.unpack(">i", compressed[:4])[0]
        sub_size = struct.unpack(">i", compressed[4:8])[0]
        assert block_size == len(data)
        assert len(compressed) == 8 + sub_size


class TestFraming2:
    def test_roundtrip(self, uncompressed):
        c = Compressor()
        compressed = c.compress(uncompressed)
        c.flush()
        d = Decompressor()
        result = d.decompress(compressed)
        d.flush()
        assert result == uncompressed
