# cython: profile=False
from struct import pack, unpack
from pysnappy.crc32c cimport masked_crc32c
from pysnappy.core cimport compress as _compress
from pysnappy.core cimport uncompress as _uncompress


cdef class RawDecompressor:
    cpdef bytes decompress(self, bytes data):
        return _uncompress(data)
    cpdef flush(self):
        return b""


cdef class RawCompressor:
    cpdef bytes compress(self, bytes data):
        return _compress(data)
    cpdef flush(self):
        return b""


cdef class HadoopDecompressor:

    def __cinit__(self):
        self._buf = b""
        self._block_size = -1
        self._block_read = 0
        self._subblock_size = -1

    cpdef bytes decompress(self, bytes data):
        cdef bytes output = b""
        cdef bytes buf
        self._buf += data
        while True:
            buf = self._decompress_block()
            if len(buf) > 0:
                output += buf
            else:
                break
        return output

    cdef bytes _decompress_block(self):
        cdef bytes buf
        cdef bytes output = b""
        if self._block_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._block_size = unpack(
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        while self._block_read < self._block_size:
            buf = self._decompress_subblock()
            if len(buf) > 0:
                output += buf
            else:
                # Buffer doesn't contain full subblock.
                break
        if self._block_read == self._block_size:
            # Finished reading this block, so reinitialize
            self._block_read = 0
            self._block_size = -1
        return output

    cdef bytes _decompress_subblock(self):
        cdef bytes compressed
        cdef bytes uncompressed
        if self._subblock_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._subblock_size = unpack(
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        # Only attempt to decompress complete subblocks.
        if len(self._buf) < self._subblock_size:
            return b""
        compressed = self._buf[:self._subblock_size]
        self._buf = self._buf[self._subblock_size:]
        uncompressed = _uncompress(compressed)
        self._block_read += len(uncompressed)
        self._subblock_size = -1
        return uncompressed

    cpdef flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")
        return b""


cdef class HadoopCompressor:
    def __cinit__(self, buffer_size=131072):
        self._buffer_size = buffer_size
        self._buf = b""

    cpdef bytes add_chunk(self, bytes data):
        cdef bytes output = b""
        cdef bytes buf
        cdef int uncompressed_length
        cdef int compressed_length
        self._buf += data
        while True:
            if len(self._buf) > self._buffer_size:
                buf = self._buf[:self._buffer_size]
                self._buf = self._buf[self.buffer_size:]
            else:
                buf = self._buf[:]
                self._buf = b""
            uncompressed_length = len(buf)
            if uncompressed_length == 0:
                break
            try:
                buf = _compress(buf)
            except:
                break
            compressed_length = len(buf)
            output += pack(">i", uncompressed_length)
            output += pack(">i", compressed_length)
            output += buf
        return output

    cpdef bytes compress(self, bytes data):
        return self.add_chunk(data)

    cpdef flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")
        return b""


cdef int _CHUNK_MAX = 65536
cdef int _STREAM_TO_STREAM_BLOCK_SIZE = _CHUNK_MAX
cdef bytes _STREAM_IDENTIFIER = b"sNaPpY"
cdef int _COMPRESSED_CHUNK = 0x00
cdef int _UNCOMPRESSED_CHUNK = 0x01
cdef int _IDENTIFIER_CHUNK = 0xff
cdef int _RESERVED_UNSKIPPABLE_LEFT = 0x02  # chunk ranges are [inclusive, exclusive)
cdef int _RESERVED_UNSKIPPABLE_RIGHT = 0x80
cdef int _RESERVED_SKIPPABLE_LEFT = 0x80
cdef int _RESERVED_SKIPPABLE_RIGHT = 0xff

cdef double _COMPRESSION_THRESHOLD = 0.125

cdef class Decompressor:

    def __cinit__(self):
        self._buf = b""
        self._header_found = False

    cpdef bytes decompress(self, bytes data):
        cdef bytes output = b""
        cdef bytes chunk
        cdef bytes stream_crc
        cdef long chunk_type
        cdef int size
        self._buf += data
        while True:
            if len(self._buf) < 4:
                return output
            chunk_type = unpack("<L", self._buf[:4])[0]
            size = (chunk_type >> 8)
            chunk_type &= 0xff
            if not self._header_found:
                if (chunk_type != _IDENTIFIER_CHUNK or size != len(_STREAM_IDENTIFIER)):
                    raise Exception("Stream missing snappy identifier")
                self._header_found = True
            if (_RESERVED_UNSKIPPABLE_LEFT <= chunk_type and chunk_type < _RESERVED_UNSKIPPABLE_RIGHT):
                raise Exception("Stream received unskippable but unknown chunk: " + str(chunk_type))
            if len(self._buf) < 4 + size:
                return output
            chunk, self._buf = self._buf[4:4 + size], self._buf[4 + size:]
            if chunk_type == _IDENTIFIER_CHUNK:
                if chunk != _STREAM_IDENTIFIER:
                    raise Exception("Stream has invalid snappy identifier")
                continue
            if (_RESERVED_SKIPPABLE_LEFT <= chunk_type and
                    chunk_type < _RESERVED_SKIPPABLE_RIGHT):
                continue
            assert chunk_type in (_COMPRESSED_CHUNK, _UNCOMPRESSED_CHUNK)
            stream_crc, chunk = chunk[:4], chunk[4:4 + size]
            if chunk_type == _COMPRESSED_CHUNK:
                chunk = _uncompress(chunk)
            if pack("<L", masked_crc32c(chunk)) != stream_crc:
                raise Exception("crc mismatch: " +
                                str(pack("<L", masked_crc32c(chunk))) +
                                " expected: " +
                                str(stream_crc))
            output += chunk

    cpdef flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")
        return b""


cdef class Compressor:

    def __cinit__(self):
        self._header_chunk_written = False

    cpdef bytes add_chunk(self, bytes data, compress=None):
        cdef bytes compressed_chunk
        cdef bytes chunk
        cdef long crc
        cdef long chunk_type
        cdef bytes out = b""
        cdef int i
        if not self._header_chunk_written:
            self._header_chunk_written = True
            out += pack("<L", _IDENTIFIER_CHUNK
                                 + (len(_STREAM_IDENTIFIER) << 8))
            out += _STREAM_IDENTIFIER

        for i in range(0, len(data), _CHUNK_MAX):
            chunk = data[i:i + _CHUNK_MAX]
            crc = masked_crc32c(chunk)
            if compress is None:
                compressed_chunk = _compress(chunk)
                if (len(compressed_chunk) <=
                    (1.0 - _COMPRESSION_THRESHOLD) * len(chunk)):
                    chunk = compressed_chunk
                    chunk_type = _COMPRESSED_CHUNK
                else:
                    chunk_type = _UNCOMPRESSED_CHUNK
            elif compress:
                chunk = _compress(chunk)
                chunk_type = _COMPRESSED_CHUNK
            else:
                chunk_type = _UNCOMPRESSED_CHUNK
            out += pack("<LL",
                                 chunk_type + ((len(chunk) + 4) << 8), crc)
            out += chunk
        return out

    cpdef bytes compress(self, bytes data):
        return self.add_chunk(data)

    cpdef flush(self, mode=None):
        return b""
