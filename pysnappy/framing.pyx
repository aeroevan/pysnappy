import struct as pystruct
from pysnappy.crc32c import crc32c, masked_crc32c
from pysnappy import compress, uncompress


cdef class RawDecompressor:
    cpdef bytes decompress(self, bytes data):
        return uncompress(data)
    def flush(self):
        pass


cdef class RawCompressor:
    cpdef bytes compress(self, bytes data):
        return compress(data)
    def flush(self):
        pass


cdef class HadoopDecompressor:
    cdef bytes _buf
    cdef int _block_size
    cdef int _block_read
    cdef int _subblock_size

    def __init__(self):
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

    cpdef bytes _decompress_block(self):
        cdef bytes buf
        if self._block_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._block_size = pystruct.unpack(
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        cdef bytes output = b""
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

    cpdef bytes _decompress_subblock(self):
        cdef bytes compressed
        cdef bytes uncompressed
        if self._subblock_size < 0:
            if len(self._buf) <= 4:
                return b""
            self._subblock_size = pystruct.unpack(
                ">i", self._buf[:4])[0]
            self._buf = self._buf[4:]
        # Only attempt to decompress complete subblocks.
        if len(self._buf) < self._subblock_size:
            return b""
        compressed = self._buf[:self._subblock_size]
        self._buf = self._buf[self._subblock_size:]
        uncompressed = uncompress(compressed)
        self._block_read += len(uncompressed)
        self._subblock_size = -1
        return uncompressed

    def flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")


cdef class HadoopCompressor:
    cdef int _buffer_size
    cdef bytes _buf
    def __init__(self, buffer_size=131072):
        self._buffer_size = buffer_size
        self._buf = b""

    cpdef bytes compress(self, bytes data):
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
                buf = compress(buf)
            except:
                break
            compressed_length = len(buf)
            output += pystruct.pack(">i", uncompressed_length)
            output += pystruct.pack(">i", compressed_length)
            output += buf
        return output

    def flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")

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

cdef class Decompressor:
    cdef bytes _buf
    cdef bint _header_found

    def __init__(self):
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
            chunk_type = pystruct.unpack("<L", self._buf[:4])[0]
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
                print("Skippable")
                continue
            assert chunk_type in (_COMPRESSED_CHUNK, _UNCOMPRESSED_CHUNK)
            stream_crc, chunk = chunk[:4], chunk[4:4 + size]
            if chunk_type == _COMPRESSED_CHUNK:
                chunk = uncompress(chunk)
            if pystruct.pack("<L", masked_crc32c(chunk)) != stream_crc:
                raise Exception("crc mismatch: " +
                                str(pystruct.pack("<L", masked_crc32c(chunk))) +
                                " expected: " +
                                str(stream_crc))
            output += chunk

    def flush(self):
        if len(self._buf) > 0:
            raise Exception("Chunk truncated")
