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
    @pytest.fixture
    def compressed(self):
        return (DATA_DIR / "iris.csv.raw").read_bytes()

    def test_uncompress(self, compressed, uncompressed):
        assert pysnappy.uncompress(compressed) == uncompressed

    def test_compress(self, compressed, uncompressed):
        assert pysnappy.compress(uncompressed) == compressed

    def test_framed_uncompress(self, compressed, uncompressed):
        r = RawDecompressor()
        result = r.decompress(compressed)
        r.flush()
        assert result == uncompressed

    def test_framed_compress(self, compressed, uncompressed):
        r = RawCompressor()
        result = r.compress(uncompressed)
        r.flush()
        assert result == compressed


class TestHadoop:
    @pytest.fixture
    def compressed(self):
        return (DATA_DIR / "iris.hadoop.snappy").read_bytes()

    def test_uncompress(self, compressed, uncompressed):
        h = HadoopDecompressor()
        result = h.decompress(compressed)
        h.flush()
        assert result == uncompressed

    def test_compress(self, compressed, uncompressed):
        h = HadoopCompressor()
        result = h.compress(uncompressed)
        h.flush()
        assert result == compressed


class TestFraming2:
    def test_uncompress(self):
        decompressor = Decompressor()
        with (
            (DATA_DIR / "iris.framing2.sz").open("rb") as comp_fh,
            (DATA_DIR / "iris.csv").open("rb") as uncomp_fh,
        ):
            while True:
                buf = comp_fh.read(65536)
                if not buf:
                    break
                buf = decompressor.decompress(buf)
                if buf:
                    assert buf == uncomp_fh.read(len(buf))
            decompressor.flush()

    def test_compress(self):
        compressor = Compressor()
        with (
            (DATA_DIR / "iris.framing2.sz").open("rb") as comp_fh,
            (DATA_DIR / "iris.csv").open("rb") as uncomp_fh,
        ):
            while True:
                buf = uncomp_fh.read(65536)
                if not buf:
                    break
                buf = compressor.compress(buf)
                if buf:
                    assert buf == comp_fh.read(len(buf))
            compressor.flush()
